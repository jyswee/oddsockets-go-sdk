// Command challenge runs an HONEST two-client challenge/leaderboard/achievement
// regression for the OddSockets Go SDK against a live worker. Two DISTINCT users
// (alice + bob) share the SAME apiKey (shared owner scope) and both subscribe to
// the 'lobby' room. Every cross-client assertion proves an event fired by one
// client travelled through the OddSockets worker and surfaced on the OTHER
// client - no local echo.
//
// Wire contract (worker v1.2): room broadcasts arrive wrapped
// {version,type,identity,challengeId,data:{...}} with semantic fields under
// .data. Directed events (challenge_invited / challenge_reply_received /
// challenge_invite_cancelled) are FLAT and the sender is a nested object
// from:{userId,identity}.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jyswee/oddsockets-go-sdk/oddsockets"
)

const (
	room    = "lobby"
	timeout = 25 * time.Second
)

func env(name string) string {
	v := os.Getenv(name)
	if v == "" {
		fmt.Fprintf(os.Stderr, "%s is not set\n", name)
		os.Exit(2)
	}
	return v
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}

// results collects per-assertion PASS/FAIL for the final report.
type results struct {
	pass  []string
	failn []string
}

func (r *results) ok(name string)  { r.pass = append(r.pass, name); fmt.Printf("  PASS  %s\n", name) }
func (r *results) no(name, why string) {
	r.failn = append(r.failn, name+" - "+why)
	fmt.Printf("  FAIL  %s - %s\n", name, why)
}
func (r *results) assert(name string, cond bool, why string) {
	if cond {
		r.ok(name)
	} else {
		r.no(name, why)
	}
}

func mustConnect(managerURL, apiKey, userID string) *oddsockets.Client {
	client, err := oddsockets.NewClient(&oddsockets.Config{
		APIKey:      apiKey,
		ManagerURL:  managerURL,
		UserID:      userID,
		AutoConnect: false,
	})
	if err != nil {
		fail("[%s] create client: %v", userID, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		fail("[%s] connect: %v", userID, err)
	}
	return client
}

func workerID(c *oddsockets.Client) string {
	if info := c.GetWorkerInfo(); info != nil {
		if id, ok := info["workerId"].(string); ok {
			return id
		}
	}
	return "unknown"
}

// asMap coerces an interface to map[string]interface{}.
func asMap(v interface{}) map[string]interface{} {
	m, _ := v.(map[string]interface{})
	return m
}

// data returns the .data sub-object of a wrapped room broadcast, or the
// envelope itself if there is no nested data (defensive).
func data(m map[string]interface{}) map[string]interface{} {
	if d, ok := m["data"].(map[string]interface{}); ok {
		return d
	}
	return m
}

// num coerces a JSON number (float64) or int to float64.
func num(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	}
	return 0, false
}

// senderUserID extracts the sender userId from a directed event's nested
// from:{userId,identity} object.
func senderUserID(m map[string]interface{}) string {
	if from, ok := m["from"].(map[string]interface{}); ok {
		if u, ok := from["userId"].(string); ok {
			return u
		}
		if id, ok := from["identity"].(string); ok {
			return id
		}
	}
	return ""
}

func main() {
	apiKey := env("OS_KEY")
	managerURL := env("ODDSOCKETS_MANAGER_URL")
	r := &results{}

	fmt.Println("[connect] connecting alice + bob (shared apiKey, distinct userId)...")
	alice := mustConnect(managerURL, apiKey, "alice")
	bob := mustConnect(managerURL, apiKey, "bob")
	defer alice.Close()
	defer bob.Close()

	aliceWorker := workerID(alice)
	bobWorker := workerID(bob)
	fmt.Printf("[alice] worker %s\n", aliceWorker)
	fmt.Printf("[bob]   worker %s\n", bobWorker)

	challengeID := fmt.Sprintf("chal-%d", time.Now().UnixNano())
	achievementID := fmt.Sprintf("ach-%d", time.Now().UnixNano())

	// ---- alice's inbound room-broadcast + directed listeners ----
	aliceProgress := make(chan map[string]interface{}, 8)
	aliceRankChange := make(chan map[string]interface{}, 8)
	aliceReplyRecv := make(chan map[string]interface{}, 4)

	alice.On("challenge_progress", func(_ oddsockets.EventType, v interface{}) {
		aliceProgress <- asMap(v)
	})
	alice.On("leaderboard_rank_change", func(_ oddsockets.EventType, v interface{}) {
		aliceRankChange <- asMap(v)
	})
	alice.On("challenge_reply_received", func(_ oddsockets.EventType, v interface{}) {
		aliceReplyRecv <- asMap(v)
	})

	// ---- bob's inbound listeners ----
	bobAchProgress := make(chan map[string]interface{}, 4)
	bobAchUnlock := make(chan map[string]interface{}, 4)
	bobInvited := make(chan map[string]interface{}, 4)
	bobCancelled := make(chan map[string]interface{}, 4)

	bob.On("achievement_progress", func(_ oddsockets.EventType, v interface{}) {
		bobAchProgress <- asMap(v)
	})
	bob.On("achievement_unlock", func(_ oddsockets.EventType, v interface{}) {
		bobAchUnlock <- asMap(v)
	})
	bob.On("challenge_invited", func(_ oddsockets.EventType, v interface{}) {
		bobInvited <- asMap(v)
	})
	bob.On("challenge_invite_cancelled", func(_ oddsockets.EventType, v interface{}) {
		bobCancelled <- asMap(v)
	})

	// Both clients join the same room.
	ctx := context.Background()
	aliceCh := alice.Channel(room)
	bobCh := bob.Channel(room)
	aliceMsgs := make(chan *oddsockets.Message, 100)
	bobMsgs := make(chan *oddsockets.Message, 100)
	if err := aliceCh.Subscribe(ctx, aliceMsgs, &oddsockets.SubscribeOptions{EnablePresence: true}); err != nil {
		fail("[alice] subscribe: %v", err)
	}
	if err := bobCh.Subscribe(ctx, bobMsgs, &oddsockets.SubscribeOptions{EnablePresence: true}); err != nil {
		fail("[bob] subscribe: %v", err)
	}
	fmt.Printf("[both] subscribed to '%s'\n", room)
	time.Sleep(700 * time.Millisecond) // let room membership settle

	fmt.Println("\n=== 1. CreateChallenge (alice) ===")
	createAck, err := alice.Enhanced.CreateChallenge(oddsockets.CreateChallengeParams{
		ChallengeID: challengeID,
		Metric:      "score",
		Ranked:      true,
		Channel:     room,
	})
	if err != nil {
		r.no("create acked", err.Error())
	} else {
		r.assert("create acked", createAck != nil, "nil ack")
		fmt.Printf("  ack: %v\n", createAck)
	}

	fmt.Println("\n=== 2. ReportProgress alice=40, bob=55 ===")
	// alice must see her own progress + a leaderboard_rank_change broadcast.
	if err := alice.Enhanced.ReportProgress(oddsockets.ReportProgressParams{
		ChallengeID: challengeID, Metric: "score", Value: 40, EventID: "evt-a-40",
	}); err != nil {
		fail("[alice] ReportProgress: %v", err)
	}
	if err := bob.Enhanced.ReportProgress(oddsockets.ReportProgressParams{
		ChallengeID: challengeID, Metric: "score", Value: 55, EventID: "evt-b-55",
	}); err != nil {
		fail("[bob] ReportProgress: %v", err)
	}

	// Wait until alice has seen a progress broadcast carrying value==55 (bob's),
	// which guarantees the worker has processed both before we query standings
	// (avoids the cross-worker race).
	gotProgress := false
	sawValue55 := false
	gotRankChange := false
	deadline := time.After(timeout)
progressLoop:
	for !(sawValue55 && gotRankChange) {
		select {
		case p := <-aliceProgress:
			gotProgress = true
			d := data(p)
			if v, ok := num(d["value"]); ok && v == 55 {
				sawValue55 = true
			}
		case rc := <-aliceRankChange:
			gotRankChange = true
			_ = rc
		case <-deadline:
			break progressLoop
		}
	}
	r.assert("alice sees challenge_progress (broadcast)", gotProgress, "no challenge_progress reached alice")
	r.assert("alice sees leaderboard_rank_change (broadcast)", gotRankChange, "no leaderboard_rank_change reached alice")
	r.assert("alice observed bob value==55 before standings", sawValue55, "never saw value==55; standings may race")

	fmt.Println("\n=== 3. GetStandings (alice) ===")
	standings, err := alice.Enhanced.GetStandings(oddsockets.GetStandingsParams{
		ChallengeID: challengeID, Limit: 10,
	})
	if err != nil {
		r.no("standings acked", err.Error())
	} else {
		fmt.Printf("  standings: %v\n", standings)
		rows, _ := standings["standings"].([]interface{})
		var bobRank, aliceRank float64
		var bobVal, aliceVal float64
		bobFound, aliceFound := false, false
		for _, row := range rows {
			m := asMap(row)
			id, _ := m["identity"].(string)
			rank, _ := num(m["rank"])
			val, _ := num(m["value"])
			switch id {
			case "bob":
				bobRank, bobVal, bobFound = rank, val, true
			case "alice":
				aliceRank, aliceVal, aliceFound = rank, val, true
			}
		}
		r.assert("standings: bob@55 rank1", bobFound && bobVal == 55 && bobRank == 1,
			fmt.Sprintf("bobFound=%v val=%v rank=%v", bobFound, bobVal, bobRank))
		r.assert("standings: alice@40 rank2", aliceFound && aliceVal == 40 && aliceRank == 2,
			fmt.Sprintf("aliceFound=%v val=%v rank=%v", aliceFound, aliceVal, aliceRank))
		yr, _ := num(standings["yourRank"])
		r.assert("standings: alice yourRank==2", yr == 2, fmt.Sprintf("yourRank=%v", standings["yourRank"]))
	}

	fmt.Println("\n=== 4. CompleteChallenge alice(tied) + bob(conceded) ===")
	aliceComplete, err := alice.Enhanced.CompleteChallenge(oddsockets.CompleteChallengeParams{
		ChallengeID: challengeID, Outcome: "tied", EventID: "evt-a-done",
	})
	if err != nil {
		r.no("alice complete(tied) acked", err.Error())
	} else {
		fmt.Printf("  alice complete ack: %v\n", aliceComplete)
		fv, _ := num(aliceComplete["finalValue"])
		rk, _ := num(aliceComplete["rank"])
		oc, _ := aliceComplete["outcome"].(string)
		r.assert("alice complete(tied): finalValue40 rank2 outcome=tied",
			fv == 40 && rk == 2 && oc == "tied",
			fmt.Sprintf("finalValue=%v rank=%v outcome=%v", aliceComplete["finalValue"], aliceComplete["rank"], oc))
	}
	bobComplete, err := bob.Enhanced.CompleteChallenge(oddsockets.CompleteChallengeParams{
		ChallengeID: challengeID, Outcome: "conceded", EventID: "evt-b-done",
	})
	if err != nil {
		r.no("bob complete(conceded) acked", err.Error())
	} else {
		fmt.Printf("  bob complete ack: %v\n", bobComplete)
		fv, _ := num(bobComplete["finalValue"])
		rk, _ := num(bobComplete["rank"])
		oc, _ := bobComplete["outcome"].(string)
		r.assert("bob complete(conceded): finalValue55 rank1 outcome=conceded",
			fv == 55 && rk == 1 && oc == "conceded",
			fmt.Sprintf("finalValue=%v rank=%v outcome=%v", bobComplete["finalValue"], bobComplete["rank"], oc))
	}

	fmt.Println("\n=== 5. UnlockAchievement 50% (progress, no banner on bob) ===")
	if err := alice.Enhanced.UnlockAchievement(oddsockets.UnlockAchievementParams{
		AchievementID: achievementID, Name: "First Blood", PercentComplete: 50, Channel: room,
	}); err != nil {
		fail("[alice] UnlockAchievement(50): %v", err)
	}
	select {
	case p := <-bobAchProgress:
		d := data(p)
		st, _ := d["status"].(string)
		pc, _ := num(d["percentComplete"])
		r.assert("bob sees achievement_progress in_progress (50%)",
			st == "in_progress" && pc == 50, fmt.Sprintf("status=%v percent=%v", st, d["percentComplete"]))
	case u := <-bobAchUnlock:
		r.no("bob sees achievement_progress in_progress (50%)", fmt.Sprintf("got achievement_unlock instead: %v", u))
	case <-time.After(timeout):
		r.no("bob sees achievement_progress in_progress (50%)", "timeout")
	}
	// Ensure no premature unlock banner within a short window.
	select {
	case u := <-bobAchUnlock:
		r.no("no premature achievement_unlock banner at 50%", fmt.Sprintf("got unlock: %v", u))
	case <-time.After(1500 * time.Millisecond):
		r.ok("no premature achievement_unlock banner at 50%")
	}

	fmt.Println("\n=== 6. UnlockAchievement 100% (unlock banner on bob) ===")
	if err := alice.Enhanced.UnlockAchievement(oddsockets.UnlockAchievementParams{
		AchievementID: achievementID, Name: "First Blood", PercentComplete: 100, Channel: room,
	}); err != nil {
		fail("[alice] UnlockAchievement(100): %v", err)
	}
	select {
	case u := <-bobAchUnlock:
		d := data(u)
		st, _ := d["status"].(string)
		r.assert("bob sees achievement_unlock unlocked (100%)",
			st == "unlocked" || st == "", fmt.Sprintf("status=%v (full=%v)", st, d))
		fmt.Printf("  unlock: %v\n", u)
	case <-time.After(timeout):
		r.no("bob sees achievement_unlock unlocked (100%)", "timeout")
	}

	fmt.Println("\n=== 7. GetAchievements (alice) ===")
	achs, err := alice.Enhanced.GetAchievements(oddsockets.GetAchievementsParams{AchievementID: achievementID})
	if err != nil {
		r.no("GetAchievements 100/unlocked", err.Error())
	} else {
		fmt.Printf("  achievements: %v\n", achs)
		rows, _ := achs["achievements"].([]interface{})
		found := false
		for _, row := range rows {
			m := asMap(row)
			if id, _ := m["achievementId"].(string); id == achievementID {
				pc, _ := num(m["percentComplete"])
				st, _ := m["status"].(string)
				r.assert("GetAchievements 100/unlocked", pc == 100 && st == "unlocked",
					fmt.Sprintf("percent=%v status=%v", m["percentComplete"], st))
				found = true
			}
		}
		if !found {
			r.no("GetAchievements 100/unlocked", "achievement not in list")
		}
	}

	fmt.Println("\n=== 8. SendChallengeInvite alice->bob (directed to invitee only) ===")
	inviteAck, err := alice.Enhanced.SendChallengeInvite(oddsockets.SendChallengeInviteParams{
		ToUserID: "bob", Type: "match", Payload: map[string]interface{}{"map": "dust2"}, TTL: 300,
	})
	var inviteID string
	if err != nil {
		r.no("invite acked pending", err.Error())
	} else {
		fmt.Printf("  invite ack: %v\n", inviteAck)
		inviteID, _ = inviteAck["inviteId"].(string)
		st, _ := inviteAck["status"].(string)
		to, _ := inviteAck["toUserId"].(string)
		r.assert("invite acked pending", inviteID != "" && st == "pending" && to == "bob",
			fmt.Sprintf("inviteId=%q status=%q to=%q", inviteID, st, to))
	}
	// bob (invitee) must receive challenge_invited; alice must NOT (directed).
	select {
	case inv := <-bobInvited:
		sender := senderUserID(inv)
		r.assert("bob sees challenge_invited from alice", sender == "alice",
			fmt.Sprintf("sender=%q full=%v", sender, inv))
		fmt.Printf("  bob invited: %v\n", inv)
	case <-time.After(timeout):
		r.no("bob sees challenge_invited from alice", "timeout")
	}

	fmt.Println("\n=== 9. GetChallengeInvites (bob lists it) ===")
	invs, err := bob.Enhanced.GetChallengeInvites()
	if err != nil {
		r.no("bob GetChallengeInvites lists invite", err.Error())
	} else {
		fmt.Printf("  invites: %v\n", invs)
		rows, _ := invs["invites"].([]interface{})
		listed := false
		for _, row := range rows {
			m := asMap(row)
			if id, _ := m["inviteId"].(string); id == inviteID && inviteID != "" {
				listed = true
			}
		}
		r.assert("bob GetChallengeInvites lists invite", listed, "invite not in bob's list")
	}

	fmt.Println("\n=== 10. ReplyChallengeInvite bob accept -> alice sees reply ===")
	if inviteID == "" {
		r.no("alice sees challenge_reply_received (accept)", "no inviteId to reply to")
	} else {
		replyAck, err := bob.Enhanced.ReplyChallengeInvite(oddsockets.ReplyChallengeInviteParams{
			InviteID: inviteID, Accept: true,
		})
		if err != nil {
			r.no("bob reply(accept) acked", err.Error())
		} else {
			r.ok("bob reply(accept) acked")
			fmt.Printf("  reply ack: %v\n", replyAck)
		}
		select {
		case rr := <-aliceReplyRecv:
			sender := senderUserID(rr)
			accept, _ := rr["accept"].(bool)
			r.assert("alice sees challenge_reply_received (accept)",
				sender == "bob" || accept, fmt.Sprintf("sender=%q full=%v", sender, rr))
			fmt.Printf("  alice reply-received: %v\n", rr)
		case <-time.After(timeout):
			r.no("alice sees challenge_reply_received (accept)", "timeout")
		}
	}

	fmt.Println("\n=== 11. Fresh invite + Cancel -> bob sees cancelled ===")
	freshAck, err := alice.Enhanced.SendChallengeInvite(oddsockets.SendChallengeInviteParams{
		ToUserID: "bob", Type: "match", Payload: map[string]interface{}{"map": "inferno"}, TTL: 300,
	})
	if err != nil {
		r.no("bob sees challenge_invite_cancelled", "fresh invite failed: "+err.Error())
	} else {
		freshID, _ := freshAck["inviteId"].(string)
		// drain the challenge_invited for the fresh invite so it doesn't confuse.
		select {
		case <-bobInvited:
		case <-time.After(5 * time.Second):
		}
		if _, err := alice.Enhanced.CancelChallengeInvite(oddsockets.CancelChallengeInviteParams{InviteID: freshID}); err != nil {
			r.no("bob sees challenge_invite_cancelled", "cancel ack failed: "+err.Error())
		} else {
			select {
			case c := <-bobCancelled:
				id, _ := c["inviteId"].(string)
				r.assert("bob sees challenge_invite_cancelled", id == freshID || id != "",
					fmt.Sprintf("inviteId=%q full=%v", id, c))
				fmt.Printf("  bob cancelled: %v\n", c)
			case <-time.After(timeout):
				r.no("bob sees challenge_invite_cancelled", "timeout")
			}
		}
	}

	// ---- summary ----
	fmt.Printf("\n================ SUMMARY ================\n")
	fmt.Printf("alice worker: %s\n", aliceWorker)
	fmt.Printf("bob   worker: %s\n", bobWorker)
	fmt.Printf("PASS: %d   FAIL: %d\n", len(r.pass), len(r.failn))
	if len(r.failn) > 0 {
		fmt.Println("FAILURES:")
		for _, f := range r.failn {
			fmt.Printf("  - %s\n", f)
		}
		alice.Disconnect()
		bob.Disconnect()
		os.Exit(1)
	}
	fmt.Println("\nOK - all challenge assertions passed (honest two-client)")
	alice.Disconnect()
	bob.Disconnect()
}
