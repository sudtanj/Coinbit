package main

import (
	walletServices "Coinbit/wallet"
	"net/http"

	"github.com/gin-gonic/gin"
)

type DepositPayload struct {
	Amount uint64 `json:"amount"`
}

func main() {
	// When this example is run the first time, wait for creation of all internal topics (this is done
	// by goka.NewProcessor)
	walletServices.Initialize()

	r := gin.Default()
	r.POST("/wallet/:id", func(context *gin.Context) {
		id := context.Param("id")
		var depositPayload DepositPayload
		context.ShouldBindJSON(&depositPayload)

		walletServices.Deposit(id, depositPayload.Amount)

		context.JSON(http.StatusOK, gin.H{
			"message": "success deposit!",
		})
	})
	r.GET("/wallet/:id", func(c *gin.Context) {
		id := c.Param("id")
		result := walletServices.GetData(id)

		c.JSON(http.StatusOK, result)
	})
	r.Run(":8080") // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
}
