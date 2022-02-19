package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
)

var logger TransactionLogger

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	// keyValuePutHandler ожидает получить PUT-запрос с
	//ресурсом "/v1/key/{key}".
	vars := mux.Vars(r)
	key := vars["key"]
	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w,
			err.Error(),
			// Получить ключ из запроса
			// Тело запроса хранит значение
			// Если возникла ошибка, сообщить о ней
			http.StatusInternalServerError)
		return
	}
	err = Put(key, string(value))
	if err != nil {
		// Сохранить значение как строку
		// Если возникла ошибка, сообщить о ней
		http.Error(w, err.Error(),
			http.StatusInternalServerError)
		return
	}
	logger.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated) // Все хорошо! Вернуть StatusCreated
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r) // Извлечь ключ из запроса
	key := vars["key"]
	value, err := Get(key) // Получить значение для данного ключа
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(value)) // Записать значение в ответ
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	// keyValuePutHandler ожидает получить PUT-запрос с
	//ресурсом "/v1/key/{key}".
	vars := mux.Vars(r)
	key := vars["key"]
	err := Delete(key)
	if err != nil {
		// Если возникла ошибка при удалении, сообщить о ней
		http.Error(w, err.Error(),
			http.StatusInternalServerError)
		return
	}
	logger.WriteDelete(key)
	w.WriteHeader(http.StatusNoContent) // Все хорошо! Вернуть StatusNoContent
}

func keyValueGetAllKeysHandler(w http.ResponseWriter, r *http.Request) {
	value, err := GetAll() // Получить значение всего хранилища
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	j, err := json.Marshal(value)
	if err != nil {
		fmt.Printf("Error: %s", err.Error())
	}
	w.Write(j) // Записать значение в ответ
}

func initializeTransactionLog() error {
	var err error
	logger, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}
	events, errors := logger.ReadEvents()
	e, ok := Event{}, true
	for ok && err == nil {
		select {
		case err, ok = <-errors: // Получает ошибки
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete: // Получено событие DELETE!
				err = Delete(e.Key)
			case EventPut: // Получено событие PUT!
				err = Put(e.Key, e.Value)
			}
		}
	}
	logger.Run()
	defer logger.Close()
	return err
}

func main() {
	err := initializeTransactionLog()
	if err != nil {
		return
	}
	r := mux.NewRouter()
	// Зарегистрировать keyValuePutHandler как обработчик HTTP-запросов PUT,
	//в которых указан путь "/v1/{key}"
	r.HandleFunc("/v1/keys/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/keys/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/keys/{key}", keyValueDeleteHandler).Methods("DELETE")
	r.HandleFunc("/v1/keys", keyValueGetAllKeysHandler).Methods("GET")
	log.Fatal(http.ListenAndServe(":8080", r))
}
