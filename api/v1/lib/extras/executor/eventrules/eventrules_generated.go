package eventrules

// go generate -import github.com/mesos/mesos-go/api/v1/lib/executor -type E:*executor.Event:&executor.Event{}
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/mesos/mesos-go/api/v1/lib/executor"
)

type (
	evaler interface {
		// Eval executes a filter, rule, or decorator function; if the returned event is nil then
		// no additional rule evaluation should be processed for the event.
		// Eval implementations should not modify the given event parameter (to avoid side effects).
		// If changes to the event object are needed, the suggested approach is to make a copy,
		// modify the copy, and pass the copy to the chain.
		// Eval implementations SHOULD be safe to execute concurrently.
		Eval(context.Context, *executor.Event, error, Chain) (context.Context, *executor.Event, error)
	}

	// Rule is the functional adaptation of evaler.
	// A nil Rule is valid: it is Eval'd as a noop.
	Rule func(context.Context, *executor.Event, error, Chain) (context.Context, *executor.Event, error)

	// Chain is invoked by a Rule to continue processing an event. If the chain is not invoked,
	// no additional rules are processed.
	Chain func(context.Context, *executor.Event, error) (context.Context, *executor.Event, error)

	// Rules is a list of rules to be processed, in order.
	Rules []Rule

	// ErrorList accumulates errors that occur while processing a Chain of Rules. Accumulated
	// errors should be appended to the end of the list. An error list should never be empty.
	// Callers should use the package Error() func to properly accumulate (and flatten) errors.
	ErrorList []error
)

var (
	_ = evaler(Rule(nil))
	_ = evaler(Rules{})

	// chainIdentity is a Chain that returns the arguments as its results.
	chainIdentity = func(ctx context.Context, e *executor.Event, err error) (context.Context, *executor.Event, error) {
		return ctx, e, err
	}
)

// Eval is a convenience func that processes a nil Rule as a noop.
func (r Rule) Eval(ctx context.Context, e *executor.Event, err error, ch Chain) (context.Context, *executor.Event, error) {
	if r != nil {
		return r(ctx, e, err, ch)
	}
	return ch(ctx, e, err)
}

// Eval is a Rule func that processes the set of all Rules. If there are no rules in the
// set then control is simply passed to the Chain.
func (rs Rules) Eval(ctx context.Context, e *executor.Event, err error, ch Chain) (context.Context, *executor.Event, error) {
	return ch(rs.Chain()(ctx, e, err))
}

// Chain returns a Chain that evaluates the given Rules, in order, propagating the (context.Context, *executor.Event, error)
// from Rule to Rule. Chain is safe to invoke concurrently.
func (rs Rules) Chain() Chain {
	if len(rs) == 0 {
		return chainIdentity
	}
	return func(ctx context.Context, e *executor.Event, err error) (context.Context, *executor.Event, error) {
		return rs[0].Eval(ctx, e, err, rs[1:].Chain())
	}
}

// It is the semantic equivalent of Rules{r1, r2, ..., rn}.Rule() and exists purely for convenience.
func Concat(rs ...Rule) Rule { return Rules(rs).Eval }

// Error implements error; returns the message of the first error in the list.
func (es ErrorList) Error() string {
	switch len(es) {
	case 0:
		return "no errors"
	case 1:
		return es[0].Error()
	default:
		return fmt.Sprintf("%s (and %d more errors)", es[0], len(es)-1)
	}
}

// Error2 aggregates the given error params, returning nil if both are nil.
// Use Error2 to avoid the overhead of creating a slice when aggregating only 2 errors.
func Error2(a, b error) error {
	if a == nil {
		if b == nil {
			return nil
		}
		if list, ok := b.(ErrorList); ok {
			return flatten(list).Err()
		}
		return b
	}
	if b == nil {
		if list, ok := a.(ErrorList); ok {
			return flatten(list).Err()
		}
		return a
	}
	return Error(a, b)
}

// Err reduces an empty or singleton error list
func (es ErrorList) Err() error {
	if len(es) == 0 {
		return nil
	}
	if len(es) == 1 {
		return es[0]
	}
	return es
}

// IsErrorList returns true if err is a non-nil error list
func IsErrorList(err error) bool {
	if err != nil {
		_, ok := err.(ErrorList)
		return ok
	}
	return false
}

// Error aggregates, and then flattens, a list of errors accrued during rule processing.
// Returns nil if the given list of errors is empty or contains all nil errors.
func Error(es ...error) error {
	return flatten(es).Err()
}

func flatten(errors []error) ErrorList {
	if errors == nil || len(errors) == 0 {
		return nil
	}
	result := make([]error, 0, len(errors))
	for _, err := range errors {
		if err != nil {
			if multi, ok := err.(ErrorList); ok {
				result = append(result, flatten(multi)...)
			} else {
				result = append(result, err)
			}
		}
	}
	return ErrorList(result)
}

// TODO(jdef): other ideas for Rule decorators: When(func() bool), WhenNot(func() bool)

// If only executes the receiving rule if b is true; otherwise, the returned rule is a noop.
func (r Rule) If(b bool) Rule {
	if b {
		return r
	}
	return nil
}

// Unless only executes the receiving rule if b is false; otherwise, the returned rule is a noop.
func (r Rule) Unless(b bool) Rule {
	if !b {
		return r
	}
	return nil
}

// Once returns a Rule that executes the receiver only once.
func (r Rule) Once() Rule {
	if r == nil {
		return nil
	}
	var once sync.Once
	return func(ctx context.Context, e *executor.Event, err error, ch Chain) (context.Context, *executor.Event, error) {
		ruleInvoked := false
		once.Do(func() {
			ctx, e, err = r(ctx, e, err, ch)
			ruleInvoked = true
		})
		if !ruleInvoked {
			ctx, e, err = ch(ctx, e, err)
		}
		return ctx, e, err
	}
}

type Overflow int

const (
	// OverflowDiscard aborts the rule chain and returns the current state
	OverflowDiscard Overflow = iota
	// OverflowDiscardWithError aborts the rule chain and returns the current state merged with ErrOverflow
	OverflowDiscardWithError
	// OverflowBackpressure waits until the rule may execute, or the context is canceled.
	OverflowBackpressure
	// OverflowSkip skips over the decorated rule and continues processing the rule chain
	OverflowSkip
	// OverflowSkipWithError skips over the decorated rule and merges ErrOverflow upon executing the chain
	OverflowSkipWithError
)

var ErrOverflow = errors.New("overflow: rate limit exceeded")

// RateLimit invokes the receiving Rule if the chan is readable (may be closed), otherwise it handles the "overflow"
// according to the specified Overflow policy. May be useful, for example, when rate-limiting logged events.
// Returns nil (noop) if the receiver is nil, otherwise a nil chan will always trigger an overflow.
func (r Rule) RateLimit(p <-chan struct{}, over Overflow) Rule {
	if r == nil {
		return nil
	}
	return func(ctx context.Context, e *executor.Event, err error, ch Chain) (context.Context, *executor.Event, error) {
		checkTieBreaker := func() (context.Context, *executor.Event, error) {
			select {
			case <-ctx.Done():
				return ctx, e, Error2(err, ctx.Err())
			default:
				return r(ctx, e, err, ch)
			}
		}
		select {
		case <-p:
			return checkTieBreaker()
		case <-ctx.Done():
			return ctx, e, Error2(err, ctx.Err())
		default:
			// overflow
			switch over {
			case OverflowBackpressure:
				select {
				case <-p:
					return checkTieBreaker()
				case <-ctx.Done():
					return ctx, e, Error2(err, ctx.Err())
				}
			case OverflowDiscardWithError:
				return ctx, e, Error2(err, ErrOverflow)
			case OverflowDiscard:
				return ctx, e, err
			case OverflowSkipWithError:
				return ch(ctx, e, Error2(err, ErrOverflow))
			case OverflowSkip:
				return ch(ctx, e, err)
			default:
				panic(fmt.Sprintf("unexpected Overflow type: %#v", over))
			}
		}
	}
}

// EveryN invokes the receiving rule beginning with the first event seen and then every n'th
// time after that. If nthTime is less then 2 then this call is a noop (the receiver is returned).
func (r Rule) EveryN(nthTime int) Rule {
	if nthTime < 2 || r == nil {
		return r
	}
	var (
		i       = 1 // begin with the first event seen
		m       sync.Mutex
		forward = func() bool {
			m.Lock()
			i--
			if i == 0 {
				i = nthTime
				m.Unlock()
				return true
			}
			m.Unlock()
			return false
		}
	)
	return func(ctx context.Context, e *executor.Event, err error, ch Chain) (context.Context, *executor.Event, error) {
		if forward() {
			return r(ctx, e, err, ch)
		}
		return ch(ctx, e, err)
	}
}

// Drop aborts the Chain and returns the (context.Context, *executor.Event, error) tuple as-is.
func Drop() Rule {
	return Rule(nil).ThenDrop()
}

// ThenDrop executes the receiving rule, but aborts the Chain, and returns the (context.Context, *executor.Event, error) tuple as-is.
func (r Rule) ThenDrop() Rule {
	return func(ctx context.Context, e *executor.Event, err error, _ Chain) (context.Context, *executor.Event, error) {
		return r.Eval(ctx, e, err, chainIdentity)
	}
}

// Fail returns a Rule that injects the given error.
func Fail(injected error) Rule {
	return func(ctx context.Context, e *executor.Event, err error, ch Chain) (context.Context, *executor.Event, error) {
		return ch(ctx, e, Error2(err, injected))
	}
}

// DropOnError returns a Rule that generates a nil event if the error state != nil
func DropOnError() Rule {
	return Rule(nil).DropOnError()
}

// DropOnError decorates a rule by pre-checking the error state: if the error state != nil then
// the receiver is not invoked and (e, err) is returned; otherwise control passes to the receiving rule.
func (r Rule) DropOnError() Rule {
	return func(ctx context.Context, e *executor.Event, err error, ch Chain) (context.Context, *executor.Event, error) {
		if err != nil {
			return ctx, e, err
		}
		return r.Eval(ctx, e, err, ch)
	}
}

// AndThen returns a list of rules, beginning with the receiver, followed by DropOnError, and then
// all of the rules specified by the next parameter. The net effect is: execute the receiver rule
// and only if there is no error state, continue processing the next rules, in order.
func (r Rule) AndThen(next ...Rule) Rule {
	return append(Rules{r, DropOnError()}, next...).Eval
}

func DropOnSuccess() Rule {
	return Rule(nil).DropOnSuccess()
}

func (r Rule) DropOnSuccess() Rule {
	return func(ctx context.Context, e *executor.Event, err error, ch Chain) (context.Context, *executor.Event, error) {
		if err == nil {
			// bypass remainder of chain
			return ctx, e, err
		}
		return r.Eval(ctx, e, err, ch)
	}
}

func (r Rule) OnFailure(next ...Rule) Rule {
	return append(Rules{r, DropOnSuccess()}, next...).Eval
}
