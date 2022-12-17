package main

import (
	walletServices "Coinbit/wallet"
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	// When this example is run the first time, wait for creation of all internal topics (this is done
	// by goka.NewProcessor)
	initialized := make(chan struct{})
	go walletServices.Initialize(initialized)

	<-initialized

	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		walletServices.Deposit("test", 20)

		result := walletServices.GetData(initialized, "test")

		c.JSON(http.StatusOK, result)
	})
	r.Run(":8080") // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
}
