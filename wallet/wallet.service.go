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
	"time"
)

var (
	brokers                    = []string{"localhost:9092"}
	walletTopic    goka.Stream = "wallet"
	walletGroup    goka.Group  = "wallet"
	thresholdTopic goka.Stream = "threshold-test2"
	thresholdGroup goka.Group  = "threshold-test2"

	tmcWallet     *goka.TopicManagerConfig
	tmcThreshold  *goka.TopicManagerConfig
	viewWallet    *goka.View
	viewThreshold *goka.View

	waitTimeInSeconds = 120
)

func Initialize() {
	config := goka.DefaultConfig()
	// since the emitter only emits one message, we need to tell the processor
	// to read from the beginning
	// As the processor is slower to start than the emitter, it would not consume the first
	// message otherwise.
	// In production systems however, check whether you really want to read the whole walletTopic on first start, which
	// can be a lot of messages.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_4_0_0
	goka.ReplaceGlobalConfig(config)

	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmcWallet = goka.NewTopicManagerConfig()
	tmcWallet.Table.Replication = 1
	tmcWallet.Stream.Replication = 1
	tmWallet, err := goka.NewTopicManager(brokers, config, tmcWallet)
	if err != nil {
		log.Fatalf("Error creating walletTopic manager: %v", err)
	}
	defer tmWallet.Close()
	err = tmWallet.EnsureStreamExists(string(walletTopic), 8)
	if err != nil {
		log.Printf("Error creating kafka walletTopic %s: %v", walletTopic, err)
	}

	go runWalletProcessor() // press ctrl-c to stop

	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmcThreshold = goka.NewTopicManagerConfig()
	tmcThreshold.Table.Replication = 1
	tmcThreshold.Stream.Replication = 1
	//tmcThreshold.Stream.Retention = 120000
	//fmt.Println(time.Duration(5000000000).Seconds())
	//tmcThreshold.Stream.Retention = time.Duration(5000000000)
	tmThreshold, err := goka.NewTopicManager(brokers, config, tmcThreshold)
	if err != nil {
		log.Fatalf("Error creating walletTopic manager: %v", err)
	}
	defer tmThreshold.Close()
	err = tmThreshold.EnsureStreamExists(string(thresholdTopic), 8)
	if err != nil {
		log.Printf("Error creating kafka walletTopic %s: %v", thresholdTopic, err)
	}

	go runThresholdProcessor()

	viewWalletCreated, err := goka.NewView(brokers,
		goka.GroupTable(walletGroup),
		new(codec.String),
	)
	if err != nil {
		panic(err)
	}

	viewWallet = viewWalletCreated

	go viewWallet.Run(context.Background())

	viewThresholdCreated, err := goka.NewView(brokers,
		goka.GroupTable(thresholdGroup),
		new(codec.String),
	)
	if err != nil {
		panic(err)
	}

	viewThreshold = viewThresholdCreated

	viewThreshold.Run(context.Background())
}

func onWalletProcessorCallback(ctx goka.Context, msg interface{}) {
	var wallet wallets.Wallet
	if ctx.Value() == nil {
		wallet = wallets.Wallet{
			WalletId: ctx.Key(),
			Amount:   0,
		}
	} else if val := ctx.Value(); val != nil {
		proto.Unmarshal([]byte(val.(string)), &wallet)
	}

	balance, _ := strconv.ParseUint(msg.(string), 10, 64)
	wallet.Amount += balance
	result, _ := proto.Marshal(&wallet)
	ctx.SetValue(string(result))
}

func runWalletProcessor() {
	g := goka.DefineGroup(walletGroup,
		goka.Input(walletTopic, new(codec.String), onWalletProcessorCallback),
		goka.Persist(new(codec.String)),
	)
	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmcWallet)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		panic(err)
	}

	p.Run(context.Background())
}

func onThresholdProcessorCallback(ctx goka.Context, msg interface{}) {
	var wallet wallets.Wallet
	if ctx.Value() == nil {
		wallet = wallets.Wallet{
			WalletId: ctx.Key(),
			Amount:   0,
		}
	} else if val := ctx.Value(); val != nil {
		proto.Unmarshal([]byte(val.(string)), &wallet)
	}

	balance, _ := strconv.ParseInt(msg.(string), 10, 64)
	wallet.Amount += uint64(balance)
	result, _ := proto.Marshal(&wallet)
	ctx.SetValue(string(result))

	if balance > 0 {
		emitter, err := goka.NewEmitter(brokers, walletTopic, new(codec.String))
		if err != nil {
			log.Fatalf("error creating emitter: %v", err)
		}
		defer emitter.Finish()
		err = emitter.EmitSync(ctx.Key(), msg)
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}

		timer := time.NewTimer(time.Duration(waitTimeInSeconds) * time.Second)
		go func() {
			<-timer.C
			emitter, err := goka.NewEmitter(brokers, thresholdTopic, new(codec.String))
			if err != nil {
				log.Fatalf("error creating emitter: %v", err)
			}
			defer emitter.Finish()
			err = emitter.EmitSync(wallet.WalletId, strconv.Itoa(-int(balance)))
			if err != nil {
				log.Fatalf("error emitting message: %v", err)
			}
			timer.Stop()
		}()
	}
}

func runThresholdProcessor() {
	g := goka.DefineGroup(thresholdGroup,
		goka.Input(thresholdTopic, new(codec.String), onThresholdProcessorCallback),
		goka.Persist(new(codec.String)),
	)
	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmcThreshold)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		panic(err)
	}

	p.Run(context.Background())
}

func Deposit(walletId string, balance uint64) {
	emitter, err := goka.NewEmitter(brokers, thresholdTopic, new(codec.String))
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

func GetData(key string) *wallets.Wallet {
	value, err := viewWallet.Get(key)
	fmt.Println(value)
	if err != nil {
		return &wallets.Wallet{
			WalletId:       key,
			Amount:         0,
			AboveThreshold: "false",
		}
	}
	var wallet wallets.Wallet
	proto.Unmarshal([]byte(value.(string)), &wallet)

	thresholdValue, thresholdErr := viewThreshold.Get(key)
	if thresholdErr != nil {
		return &wallets.Wallet{
			WalletId:       key,
			Amount:         0,
			AboveThreshold: "false",
		}
	}
	var threshold wallets.Wallet
	proto.Unmarshal([]byte(thresholdValue.(string)), &threshold)

	resultWallet := &wallets.Wallet{
		WalletId:       wallet.WalletId,
		Amount:         wallet.Amount,
		AboveThreshold: "false",
	}

	if threshold.Amount >= 10000 {
		resultWallet.AboveThreshold = "true"
	}

	return resultWallet
}
