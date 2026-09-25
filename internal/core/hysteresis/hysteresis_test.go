package hysteresis

import (
	"testing"
	"time"
)

func TestSwitch(t *testing.T) {
	g := Gate{Cooldown: 5 * time.Minute, Margin: 0.2}
	now := time.Unix(10_000, 0)
	if v := g.Switch(now, now.Add(-time.Minute), true, 10, 100); v.Allowed {
		t.Fatal("cooldown must block")
	}
	if v := g.Switch(now, now.Add(-10*time.Minute), true, 100, 110); v.Allowed {
		t.Fatal("10% better must not switch with a 20% margin")
	}
	if v := g.Switch(now, now.Add(-10*time.Minute), true, 100, 125); !v.Allowed {
		t.Fatal("25% better should switch")
	}
	if v := g.Switch(now, time.Time{}, false, 0, 1); !v.Allowed {
		t.Fatal("first choice should be allowed")
	}
}

func TestCooldowns(t *testing.T) {
	c := NewCooldowns(time.Minute)
	now := time.Unix(0, 0)
	if !c.Ready("k", now) {
		t.Fatal("fresh key should be ready")
	}
	c.Mark("k", now)
	if c.Ready("k", now.Add(30*time.Second)) {
		t.Fatal("should be cooling down")
	}
	if !c.Ready("k", now.Add(time.Minute)) {
		t.Fatal("should be ready after period")
	}
}
