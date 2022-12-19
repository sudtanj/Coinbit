package walletServices

import (
	"Coinbit/models/wallets"
	"context"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"log"
	"strconv"
	"time"
)

var (
	brokers   = []string{"localhost:9092"}
	topics    = []goka.Stream{"wallet", "threshold"}
	groups    = []goka.Group{"wallet", "threshold"}
	callbacks = []goka.ProcessCallback{onWalletProcessorCallback, onThresholdProcessorCallback}

	views map[string]*goka.View = map[string]*goka.View{}

	waitTimeInSeconds = 120
)

// internal functions
func runProcessor(topic goka.Stream, group goka.Group, tmc *goka.TopicManagerConfig, cb goka.ProcessCallback) {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), cb),
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

	p.Run(context.Background())
}

func createTopicManagerConfig(config *sarama.Config, topic goka.Stream) *goka.TopicManagerConfig {
	// This sets the default replication to 1. If you have more then one broker
	// the default configuration can be used.
	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	tm, err := goka.NewTopicManager(brokers, config, tmc)
	if err != nil {
		log.Fatalf("Error creating walletTopic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(string(topic), 8)
	if err != nil {
		log.Printf("Error creating kafka walletTopic %s: %v", topic, err)
	}

	return tmc
}

func getView(group goka.Group) *goka.View {
	if views[string(group)] == nil {
		view, err := goka.NewView(brokers,
			goka.GroupTable(group),
			new(codec.String),
		)
		if err != nil {
			panic(err)
		}
		views[string(group)] = view
	}

	return views[string(group)]
}

func runViewBackground(group goka.Group) {
	view := getView(group)
	go view.Run(context.Background())
}

func sendEvent(topic goka.Stream, id string, val interface{}) {
	emitter, err := goka.NewEmitter(brokers, topic, new(codec.String))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish()
	err = emitter.EmitSync(id, val)
	if err != nil {
		log.Fatalf("error emitting message: %v", err)
	}
	log.Println("message emitted")
}

// callbacks
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
		sendEvent(topics[0], wallet.WalletId, msg)

		timer := time.NewTimer(time.Duration(waitTimeInSeconds) * time.Second)
		go func() {
			<-timer.C
			sendEvent(topics[1], wallet.WalletId, strconv.Itoa(-int(balance)))
			timer.Stop()
		}()
	}
}

// init function
func Initialize() {
	// replace goka global config
	config := goka.DefaultConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Version = sarama.V2_4_0_0
	goka.ReplaceGlobalConfig(config)

	for i, topic := range topics {
		tmc := createTopicManagerConfig(config, topic)

		go runProcessor(topic, groups[i], tmc, callbacks[i])
		go runViewBackground(groups[i])
	}
}

// methods
func Deposit(walletId string, balance uint64) {
	sendEvent(topics[1], walletId, strconv.Itoa(int(balance)))
}

func GetData(key string) *wallets.Wallet {
	viewWallet := getView(groups[0])
	viewThreshold := getView(groups[1])

	value, err := viewWallet.Get(key)
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
