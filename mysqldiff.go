package main

import (
    "log"

    "go-mysqldiff/cmd"
)

func main() {
    if err := cmd.Execute(); err != nil {
        log.Fatalln(err)
    }
}
