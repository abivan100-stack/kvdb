// kvcli is a tiny interactive shell for kvdb.
//
//	go run ./cmd/kvcli mydata.db
//	> set name vijay
//	> get name
//	> del name
//	> keys
//	> exit
package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"kvdb"
)

func main() {
	path := "data.db"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	db, err := kvdb.Open(path, kvdb.Options{})
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Printf("kvdb: %s (%d keys). Commands: set <k> <v> | get <k> | del <k> | keys | stats | compact | exit\n", path, db.Len())
	in := bufio.NewScanner(os.Stdin)
	for fmt.Print("> "); in.Scan(); fmt.Print("> ") {
		parts := strings.SplitN(strings.TrimSpace(in.Text()), " ", 3)
		switch strings.ToLower(parts[0]) {
		case "set":
			if len(parts) < 3 {
				fmt.Println("usage: set <key> <value>")
				continue
			}
			if err := db.Put(parts[1], []byte(parts[2])); err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println("OK")
			}
		case "get":
			if len(parts) < 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			v, err := db.Get(parts[1])
			if err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println(string(v))
			}
		case "del":
			if len(parts) < 2 {
				fmt.Println("usage: del <key>")
				continue
			}
			if err := db.Delete(parts[1]); err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println("OK")
			}
		case "keys":
			ks := db.Keys()
			sort.Strings(ks)
			fmt.Println(strings.Join(ks, "\n"))
		case "stats":
			st := db.Stats()
			fmt.Printf("keys=%d file=%dB live=%dB wasted=%dB\n", st.Keys, st.FileSize, st.LiveBytes, st.DeadBytes)
		case "compact":
			cs, err := db.Compact()
			if err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Printf("%dB -> %dB\n", cs.BeforeBytes, cs.AfterBytes)
			}
		case "exit", "quit":
			return
		case "":
		default:
			fmt.Println("unknown command")
		}
	}
}
