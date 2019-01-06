package main

import (
	"bufio"
	"container/heap"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const BUFSIZE = 4096

/* Priority Queue boiler-plate */
type Transaction struct {
	/* timestamp in micro-seconds from epoch+offset */
	starttime int64
	endtime   int64
}

type QIFHeap []*Transaction

func (h QIFHeap) Len() int            { return len(h) }
func (h QIFHeap) Peek() *Transaction  { return h[0] }
func (h QIFHeap) Less(i, j int) bool  { return h[i].endtime < h[j].endtime }
func (h QIFHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *QIFHeap) Push(x interface{}) { *h = append(*h, x.(*Transaction)) }
func (h *QIFHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type QIF struct {
	tr    *Transaction
	qif   int
	extra string
}

func errExit(msg string, code ...int) {
	log.Fatal(msg + "\n")
	if code != nil {
		os.Exit(code[0])
	} else {
		os.Exit(1)
	}
}

func exitIfError(err error) {
	if err != nil {
		errExit(err.Error())
	}
}

/*
 * parse a timestamp string in floating point string and
 * returns int64 interger of microsecs
 */
func parseTimeMicroSec(s string) (int64, error) {
	ss := strings.Split(s, ".")
	if len(ss) > 2 || len(ss) <= 0 {
		return 0, errors.New("malformed timestamp string: " + s)
	}

	frac := []byte("000000")

	if len(ss) == 2 {
		for i, digit := range ss[1][:6] {
			frac[i] = byte(digit)
		}
	}

	secs, err := strconv.ParseInt(ss[0]+string(frac), 10, 64)
	if err != nil {
		return 0, err
	}

	return secs, nil
}

func printQIF(w *bufio.Writer, tr *Transaction, qif int, extra string) {
	s := strconv.FormatInt(tr.starttime, 10)
	q := strconv.FormatInt(int64(qif), 10)
	//e := strconv.FormatInt(tr.endtime, 10)

	w.Write([]byte(s[:len(s)-6]))
	w.Write([]byte("."))
	w.Write([]byte(s[len(s)-6:]))
	w.Write([]byte(","))
	w.Write([]byte(q))
	w.Write([]byte("\n"))
}

func main() {
	r := csv.NewReader(bufio.NewReaderSize(os.Stdin, BUFSIZE))
	w := bufio.NewWriterSize(os.Stdout, BUFSIZE)

	pq := &QIFHeap{}
	heap.Init(pq)

	//input = make(chan (record []string, err error), BUFSIZE)
	output := make(chan QIF, BUFSIZE)
	sync := make(chan bool)

	go func() {
		for qif := range output {
			printQIF(w, qif.tr, qif.qif, qif.extra)
		}
		sync <- true
	}()

	lineNo := 1
	prev := int64(0)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		exitIfError(err)
		if len(record) < 2 {
			msg := "Less than 2 columns in the input at line %d"
			errExit(fmt.Sprintf(msg, lineNo))
		}
		start, err := parseTimeMicroSec(record[0])
		exitIfError(err)
		if start < prev {
			errExit(fmt.Sprintf("unsorted start time %d at line %d", record[0], lineNo))
		}
		end, err := parseTimeMicroSec(record[1])
		exitIfError(err)
		if start > end {
			errExit(fmt.Sprintf("starttime %s > endtime %s at line %d",
				record[0], record[1], lineNo))
		}
		for pq.Len() > 0 && pq.Peek().endtime < start {
			_ = heap.Pop(pq).(*Transaction)
		}
		newTr := &Transaction{start, end}
		heap.Push(pq, newTr)
		//printQIF(newTr, pq.Len(), "")
		output <- QIF{newTr, pq.Len(), ""}

		lineNo++
	}
	close(output)
	/*
		for pq.Len() > 0 {
			transaction := heap.Pop(pq).(*Transaction)
			printQIF(transaction, pq.Len(), "")
		}
	*/
	<-sync
	w.Flush()
	os.Exit(0)
}
