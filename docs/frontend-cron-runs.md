# Frontend fixes: cron runs stuck in "running"

Server-side context (already fixed and deployed, `a17c75c`): a run whose worker
was killed mid-run (gateway/dashboard restart) never gets `ended_at`, so the API
used to report it `running` forever. The API now flips such runs to a new
terminal status **`stale`** after 15 minutes of silence. The remaining work is
in the app: it fetched run state once and discarded the live signals that should
have refreshed it.

Three fixes, in priority order:

## 1. Handle the new `stale` run status — `Models.swift`

`RoutineRun.status` vocabulary is now `running | ok | error | stale`
(see API.md "GET /routines/{id}/runs").

- `stale` is **terminal**: the run is dead and will never finish. Render it as
  a failure variant (e.g. "Interrupted"), not a spinner, and never poll it.
- Current code has `isOk` / `isRunning` computed vars; anything that treats
  "not ok and not running" as `error` will show stale runs correctly by
  accident, but add an explicit case so the label isn't misleading:

```swift
var isRunning: Bool { status == "running" }
var isStale: Bool { status == "stale" }     // killed mid-run, never finalized
```

## 2. Stop discarding cron WS events — `AppModel.swift` (~line 848)

The events loop currently does:

```swift
} else if Self.isCronSource(item.source) {
    // Routine history is shown from the Routines tab, never here.
    continue
}
```

Dropping the item from the *feed* is right, but it's also the only live signal
that a routine is executing or just finished. Use it as an invalidation signal
instead of discarding it:

- Set a `routinesDirty = true` flag (or bump a counter) when a cron-source item
  arrives.
- Debounce ~3s after the last cron item, then refresh: `GET /routines` (list)
  and, if a routine detail screen is open, `routineRuns(id)`. The debounce
  matters: the server writes `ended_at` ~2s after the run's final message, so
  refreshing immediately on the last message can still catch `running` — one
  debounced fetch usually lands the terminal state.
- If the Routines tab isn't visible, just leave the dirty flag set and refresh
  on next appearance instead of fetching in the background.

Note: every WS `messages` item now carries `source` (`"cron"`, `"engram"`,
`"cli"`, …), so `isCronSource(item.source)` works even for sessions the app has
never seen.

## 3. Poll while a visible run is `running` — `RoutinesView.swift` (`load()`)

Routine detail fetches once on appear. While any displayed run has
`status == "running"`, re-fetch `routineRuns(routineId)` every ~10–15s until it
flips to `ok | error | stale`. Stop polling when nothing is running or the view
disappears. Also re-fetch on `scenePhase == .active` — the app may have been
backgrounded across a run's entire lifetime.

With the server-side `stale` flip as backstop, the worst case for a
killed-mid-run routine is now: it shows "running" for at most 15 minutes, then
one refresh (from any of the mechanisms above) lands it on `stale`. Nothing is
stuck forever.
