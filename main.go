package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aclindsa/ofxgo"
	"github.com/lib/pq"
)

type Transaction struct {
	ID          int
	Bank        string
	BankBranch  string
	BankAccount string
	Amount      float64
	Date        time.Time
	Type        string
	Memo        string
	FITID       string
}

const dbDriver = "postgres"
const connString = "postgres://root:password@localhost:15433/bank_statement?sslmode=disable"

func NewConnection(connectionString string) (*sql.DB, error) {
	db, err := sql.Open(dbDriver, connectionString)
	if err != nil {
		return nil, fmt.Errorf("An error happened when trying to create the database connection: %w", err)
	}
	return db, nil
}

func main() {
	file, err := os.Open("data/bank_statement.ofx")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}

	resp, err := ofxgo.ParseResponse(file)
	if err != nil {
		log.Fatal(err)
	}

	db, err := NewConnection(connString)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	stmt, ok := resp.Bank[0].(*ofxgo.StatementResponse)

	if !ok {
		log.Fatal("Not possible to cast 'resp.Bank[0]' in line 39 to  '*ofxgo.StatementResponse'")
	}

	<-Flush(ctx, db, Accumulator(ctx, 50, Transform(ctx, InitProcessing(ctx, stmt.BankTranList.Transactions))))

	log.Println("Processing finished!")
}

func InitProcessing(ctx context.Context, transactions []ofxgo.Transaction) <-chan ofxgo.Transaction {
	transactionsStream := make(chan ofxgo.Transaction)
	go func() {
		defer close(transactionsStream)
		for _, transaction := range transactions {
			select {
			case transactionsStream <- transaction:
			case <-ctx.Done():
				return
			}
		}
	}()

	return transactionsStream
}

func Transform(ctx context.Context, transactionsStream <-chan ofxgo.Transaction) <-chan Transaction {
	stream := make(chan Transaction)
	go func() {
		defer close(stream)
		for ofx := range transactionsStream {
			amount, _ := ofx.TrnAmt.Float64()
			t := Transaction{
				Amount: amount,
				Date:   ofx.DtPosted.Time,
				Type:   ofx.TrnType.String(),
				Memo:   ofx.Memo.String(),
				FITID:  ofx.FiTID.String(),
			}
			select {
			case <-ctx.Done():
				return
			case stream <- t:
			}

		}
	}()
	return stream
}

func Accumulator(ctx context.Context, batchSize int, transactionsStream <-chan Transaction) <-chan []Transaction {
	stream := make(chan []Transaction)
	go func() {
		defer close(stream)
		sl := make([]Transaction, 0, 50)
		for t := range transactionsStream {
			sl = append(sl, t)
			if len(sl) == batchSize {
				select {
				case <-ctx.Done():
					return
				case stream <- sl:
					sl = make([]Transaction, 0, 50)
				}
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		if len(sl) > 0 {
			select {
			case <-ctx.Done():
				return
			case stream <- sl:
			}
		}
	}()
	return stream
}

func Flush(ctx context.Context, db *sql.DB, transactionsStream <-chan []Transaction) <-chan struct{} {
	stream := make(chan struct{})
	go func() {
		defer close(stream)
		for transactions := range transactionsStream {
			select {
			case <-ctx.Done():
				return
			default:
				err := Store(ctx, db, transactions)
				if err != nil {
					log.Println("Error when storing the transactions:", err)
				}
			}
		}
	}()
	return stream
}

func Store(ctx context.Context, db *sql.DB, transactions []Transaction) error {

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})

	if err != nil {
		return err
	}

	sqlStmt, err := tx.PrepareContext(ctx, pq.CopyIn("transactions", "bank", "bank_branch",
		"account", "amount", "date", "type", "memo", "fitid"))

	if err != nil {
		return err
	}

	for _, t := range transactions {
		_, err := sqlStmt.ExecContext(ctx, t.Bank, t.BankBranch, t.BankAccount, t.Amount, t.Date,
			t.Type, t.Memo, t.FITID)

		if err != nil {
			return err
		}
	}

	_, err = sqlStmt.Exec()
	if err != nil {
		return err
	}

	err = sqlStmt.Close()
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
