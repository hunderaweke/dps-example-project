package events

// AssistantTopic carries the events of Oro Assistant (dps_assistant): tool calls and knowledge articles.
// The names mirror dps-contracts proto/dps/assistant/v1/events.proto.
const AssistantTopic = "assistant.events"

const (
	// AssistantToolCallExecutedV1 is dps.assistant.v1.ToolCallExecuted.
	AssistantToolCallExecutedV1 Name = "assistant.tool_call.executed.v1"
	// AssistantToolCallDeniedV1 is dps.assistant.v1.ToolCallDenied.
	AssistantToolCallDeniedV1 Name = "assistant.tool_call.denied.v1"
	// AssistantArticlePublishedV1 is dps.assistant.v1.ArticlePublished.
	AssistantArticlePublishedV1 Name = "assistant.article.published.v1"
)

var assistantDomain = Domain{
	Name:  "assistant",
	Topic: AssistantTopic,
	Events: []Name{
		AssistantToolCallExecutedV1,
		AssistantToolCallDeniedV1,
		AssistantArticlePublishedV1,
	},
}
