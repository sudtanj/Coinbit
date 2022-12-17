package walletServices

import (
	"Coinbit/models/wallets"
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"log"
	"strconv"
)

var (
	brokers             = []string{"localhost:9092"}
	topic   goka.Stream = "wallet"
	group   goka.Group  = "wallet"

	tmc  *goka.TopicManagerConfig
	view *goka.View
)

func Initialize(initialized chan struct{}) {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1

	config := goka.DefaultConfig()
	// since the emitter only emits one message, we need to tell the processor
	// to read from the beginning
	// As the processor is slower to start than the emitter, it would not consume the first
	// message otherwise.
	// In production systems however, check whether you really want to read the whole topic on first start, which
	// can be a lot of messages.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_4_0_0
	goka.ReplaceGlobalConfig(config)

	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(string(topic), 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", topic, err)
	}

	go runProcessor(initialized) // press ctrl-c to stop

	<-initialized

	viewCreated, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(codec.String),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("view initialized")
	view = viewCreated

	view.Run(context.Background())
}

func onProcessorCallback(ctx goka.Context, msg interface{}) {
	var wallet wallets.Wallet
	if ctx.Value() == nil {
		wallet = wallets.Wallet{
			WalletId:       ctx.Key(),
			Amount:         0,
			AboveThreshold: false,
		}
	} else if val := ctx.Value(); val != nil {
		proto.Unmarshal([]byte(val.(string)), &wallet)
	}

	balance, _ := strconv.ParseUint(msg.(string), 10, 64)
	wallet.Amount += balance
	result, _ := proto.Marshal(&wallet)
	ctx.SetValue(string(result))
}

func runProcessor(initialized chan struct{}) {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), onProcessorCallback),
		goka.Persist(new(codec.String)),
	)
	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		panic(err)
	}

	close(initialized)

	p.Run(context.Background())
}

func Deposit(walletId string, balance uint64) {
	emitter, err := goka.NewEmitter(brokers, topic, new(codec.String))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()
	err = emitter.EmitSync(walletId, strconv.Itoa(int(balance)))
	if err != nil {
		log.Fatalf("error emitting message: %v", err)
	}
	log.Println("message emitted")
}

func GetData(initialized chan struct{}, key string) *wallets.Wallet {
	value, err := view.Get(key)
	if err != nil {
		panic(err)
	}
	var wallet wallets.Wallet
	proto.Unmarshal([]byte(value.(string)), &wallet)

	return &wallet
}
