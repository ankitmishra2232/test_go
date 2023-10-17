package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"text/template"
	"time"
	"unicode"
	"unicode/utf8"
)

const timestampFormat = "20060102150405"

const (
	idle camelSnakeStateMachine = iota
	firstAlphaNum
	alphaNum
	delimiter
)

var (
	name    = flag.String("name", "unset", "name of migration")
	service = flag.String("service", "unset", "target service")
)

var goSQLMigrationTemplate = template.Must(template.New("goose.go-migration").Parse(`
// start {{.Name}}
func up{{.CamelName}}(tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	return nil
}
func down{{.CamelName}}(tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
// end {{.Name}}
`))

type tmplVars struct {
	Version   string
	CamelName string
	Name      string
}

type camelSnakeStateMachine int

func (s camelSnakeStateMachine) next(r rune) camelSnakeStateMachine {
	switch s {
	case idle:
		if isAlphaNum(r) {
			return firstAlphaNum
		}
	case firstAlphaNum:
		if isAlphaNum(r) {
			return alphaNum
		}
		return delimiter
	case alphaNum:
		if !isAlphaNum(r) {
			return delimiter
		}
	case delimiter:
		if isAlphaNum(r) {
			return firstAlphaNum
		}
		return idle
	}
	return s
}

func main() {
	flag.Parse()

	onrampRoot, ok := os.LookupEnv("ONRAMP_ROOT")
	if !ok {
		log.Fatal("ONRAMP_ROOT unset")
	}

	if *name == "unset" {
		log.Fatal("name flag not provided")
	}

	if *service == "unset" {
		log.Fatal("service flag not provided")
	}

	targetPath := path.Join(onrampRoot, "or-backend", "go", *service, "migrations.go")
	if _, err := os.Stat(targetPath); err != nil {
		log.Fatalf("failed to stat %q: %v\n", targetPath, err)
	}

	f, err := os.OpenFile(targetPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open %q: %v\n", targetPath, err)
	}
	defer f.Close()

	version := time.Now().Format(timestampFormat)
	migrationName := fmt.Sprintf("%s_%s", version, snakeCase(*name))

	vars := tmplVars{
		Version:   version,
		CamelName: camelCase(*name),
		Name:      migrationName,
	}

	if err := goSQLMigrationTemplate.Execute(f, vars); err != nil {
		log.Fatalf("failed to append migration declaration: %v\n", err)
	}

	log.Printf("successfully appended up and down statements to %s", targetPath)
	fmt.Printf(`
Next Steps:

1. Navigate to %s and fill out the generated up and down statements
2. Add the following line in the ApplyMigrations function before the return statement:

	goose.AddNamedMigration("%s", up%s, down%s)

`, targetPath, fmt.Sprintf("%s.go", migrationName), camelCase(*name), camelCase(*name))
}

func camelCase(str string) string {
	var b strings.Builder

	stateMachine := idle
	for i := 0; i < len(str); {
		r, size := utf8.DecodeRuneInString(str[i:])
		i += size
		stateMachine = stateMachine.next(r)
		switch stateMachine {
		case firstAlphaNum:
			b.WriteRune(unicode.ToUpper(r))
		case alphaNum:
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

func snakeCase(str string) string {
	var b bytes.Buffer

	stateMachine := idle
	for i := 0; i < len(str); {
		r, size := utf8.DecodeRuneInString(str[i:])
		i += size
		stateMachine = stateMachine.next(r)
		switch stateMachine {
		case firstAlphaNum, alphaNum:
			b.WriteRune(unicode.ToLower(r))
		case delimiter:
			b.WriteByte('_')
		}
	}
	if stateMachine == idle {
		return string(bytes.TrimSuffix(b.Bytes(), []byte{'_'}))
	}
	return b.String()
}

func isAlphaNum(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}
