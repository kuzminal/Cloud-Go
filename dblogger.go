package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq" // Анонимный импорт пакета драйвера
	"sync"
)

type PostgresTransactionLogger struct {
	events chan<- Event    // Канал только для записи; для передачи событий
	errors <-chan error    // Канал только для чтения; для приема ошибок
	db     *sql.DB         // Интерфейс доступа к базе данных
	wg     *sync.WaitGroup // для того, чтобы не потерять события
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}
func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventDelete, Key: key}
}
func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *PostgresTransactionLogger) Wait() {
	l.wg.Wait()
}

func (l *PostgresTransactionLogger) Close() error {
	l.wg.Wait()

	if l.events != nil {
		close(l.events) // Terminates Run loop and goroutine
	}

	return l.db.Close()
}

type PostgresDBParams struct {
	dbName   string
	host     string
	user     string
	password string
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transactions"

	var result string

	rows, err := l.db.Query(fmt.Sprintf("SELECT to_regclass('public.%s');", table))
	defer rows.Close()
	if err != nil {
		return false, err
	}

	for rows.Next() && result != table {
		rows.Scan(&result)
	}

	return result == table, rows.Err()
}

func (l *PostgresTransactionLogger) createTable() error {
	var err error

	createQuery := `CREATE TABLE transactions (
		sequence      BIGSERIAL PRIMARY KEY,
		event_type    SMALLINT,
		key 		  TEXT,
		value         TEXT
	  );`

	_, err = l.db.Exec(createQuery)
	if err != nil {
		return err
	}

	return nil
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16) // Создать канал событий
	l.events = events
	errors := make(chan error, 1) // Создать канал ошибок
	l.errors = errors
	go func() { // Запрос INSERT
		query := `INSERT INTO transactions
(event_type, key, value)
VALUES ($1, $2, $3)`
		for e := range events { // Извлечь следующее событие Event
			_, err := l.db.Exec( // Выполнить запрос INSERT
				query,
				e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
			}
		}
	}()
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)    // Небуферизованный канал событий
	outError := make(chan error, 1) // Буферизованный канал ошибок
	go func() {
		defer close(outEvent) // Закрыть каналы
		defer close(outError) // по завершении сопрограммы
		query := `SELECT sequence, event_type, key, value FROM transactions
ORDER BY sequence`
		rows, err := l.db.Query(query) // Выполнить запрос; получить набор результатов
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}
		defer rows.Close() // Это важно!
		e := Event{}       // Создать пустой экземпляр Event
		for rows.Next() {  // Цикл по записям
			err = rows.Scan( // Прочитать значения
				&e.Sequence, &e.EventType, // из записи в Event.
				&e.Key, &e.Value)
			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}
			outEvent <- e // Послать e в канал
		}
		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()
	return outEvent, outError
}

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable",
		config.host, config.dbName, config.user, config.password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}
	err = db.Ping() // Проверка соединения с базой данных
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}
	logger := &PostgresTransactionLogger{db: db, wg: &sync.WaitGroup{}}
	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}
	return logger, nil
}
