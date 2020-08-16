package utilities

const (
	ServerPort = 1234
	Zone       = "Rome"
	Attempts   = 10
)

type RequestArg struct {
	ID  string // id of the user
	Tag string //queue tag (name of the topic)
}

type SubscriptionOutput struct {
	QueueURL string //
}
