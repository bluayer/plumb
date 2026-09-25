// Package hysteresis prevents oscillation: a change is allowed only after a cooldown
// and only when the new choice is clearly better than the current one.
package hysteresis

import (
	"fmt"
	"sync"
	"time"
)

// Gate decides whether a change may happen now.
type Gate struct {
	Cooldown time.Duration
	// Margin is the relative improvement required to switch (0.2 = 20% better).
	Margin float64
}

// Verdict explains a gate decision.
type Verdict struct {
	Allowed bool
	Reason  string
}

// Switch decides whether to move from the current choice to a proposed one.
// hasCurrent is false when nothing is selected yet; then only the cooldown applies.
func (g Gate) Switch(now, lastChange time.Time, hasCurrent bool, currentScore, proposedScore float64) Verdict {
	if !lastChange.IsZero() && now.Sub(lastChange) < g.Cooldown {
		return Verdict{Reason: fmt.Sprintf("cooldown: %s left", (g.Cooldown - now.Sub(lastChange)).Round(time.Second))}
	}
	if !hasCurrent || currentScore <= 0 {
		return Verdict{Allowed: true, Reason: "no viable current choice"}
	}
	if proposedScore < currentScore*(1+g.Margin) {
		return Verdict{Reason: fmt.Sprintf("hysteresis: %.1f not %.0f%% better than current %.1f", proposedScore, g.Margin*100, currentScore)}
	}
	return Verdict{Allowed: true, Reason: fmt.Sprintf("%.1f beats current %.1f by margin", proposedScore, currentScore)}
}

// Cooldowns tracks per-key cooldowns in memory, e.g. one per (region, action).
type Cooldowns struct {
	Period time.Duration
	mu     sync.Mutex
	last   map[string]time.Time
}

func NewCooldowns(period time.Duration) *Cooldowns {
	return &Cooldowns{Period: period, last: map[string]time.Time{}}
}

// Ready reports whether key is out of cooldown.
func (c *Cooldowns) Ready(key string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.last[key]
	return !ok || now.Sub(t) >= c.Period
}

// Mark starts the cooldown for key.
func (c *Cooldowns) Mark(key string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last[key] = now
}

// TryMark atomically checks key against period and, if ready, starts a new cooldown.
func (c *Cooldowns) TryMark(key string, now time.Time, period time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.last[key]; ok && now.Sub(t) < period {
		return false
	}
	c.last[key] = now
	return true
}
