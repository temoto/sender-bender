package junk

import (
	"context"
	"log"
	"time"
)

func MustContextGetBool(ctx context.Context, key string) bool {
	a := ctx.Value(key)
	if a == nil {
		log.Printf("context[%s] not found", key)
		return false
	}
	if b, ok := a.(bool); ok {
		return b
	}
	if b, ok := a.(*bool); ok {
		return *b
	}
	log.Fatalf("context[%s] is not bool (%#v)", a)
	return false
}

func MustContextGetDuration(ctx context.Context, key string) time.Duration {
	a := ctx.Value(key)
	if a == nil {
		log.Fatalf("context[%s] not found", key)
		return 0
	}
	if b, ok := a.(time.Duration); ok {
		return b
	}
	if b, ok := a.(*time.Duration); ok {
		return *b
	}
	log.Fatalf("context[%s] is not time.Duration (%#v)", a)
	return 0
}

func MustContextGetInt(ctx context.Context, key string) int {
	a := ctx.Value(key)
	if a == nil {
		log.Fatalf("context[%s] not found", key)
		return 0
	}
	if b, ok := a.(int); ok {
		return b
	}
	if b, ok := a.(*int); ok {
		return *b
	}
	log.Fatalf("context[%s] is not int (%#v)", a)
	return 0
}

func MustContextGetString(ctx context.Context, key string) string {
	a := ctx.Value(key)
	if a == nil {
		log.Fatalf("context[%s] not found", key)
		return ""
	}
	if b, ok := a.(string); ok {
		return b
	}
	if b, ok := a.(*string); ok {
		return *b
	}
	if b, ok := a.([]byte); ok {
		return string(b)
	}
	log.Fatalf("context[%s] is not string or bytes (%#v)", a)
	return ""
}

func ContextSetMap(parent context.Context, m map[string]interface{}) context.Context {
	ctx := parent
	for key, value := range m {
		ctx = context.WithValue(ctx, key, value)
	}
	return ctx
}
