use std::collections::{BTreeSet, HashMap};

// Email-specific synonym groups for search expansion.
// Intentionally not loaded from JSON to avoid runtime dependencies / file I/O surprises.

fn email_synonyms() -> Vec<(&'static str, &'static [&'static str])> {
    vec![
        (
            "meeting",
            &[
                "meeting", "call", "sync", "standup", "huddle", "session", "appointment",
            ],
        ),
        ("call", &["call", "meeting", "phone", "dial", "ring", "conference"]),
        (
            "conference",
            &["conference", "meeting", "call", "webinar", "summit"],
        ),
        (
            "appointment",
            &["appointment", "meeting", "booking", "reservation", "slot"],
        ),
        ("schedule", &["schedule", "calendar", "agenda", "timetable", "plan"]),
        ("reschedule", &["reschedule", "postpone", "delay", "move", "push"]),
        ("cancel", &["cancel", "cancelled", "abort", "drop", "revoke"]),
        (
            "urgent",
            &["urgent", "asap", "immediately", "priority", "critical", "important", "rush"],
        ),
        ("asap", &["asap", "urgent", "immediately", "priority", "rush", "soon"]),
        (
            "important",
            &["important", "urgent", "priority", "critical", "key", "essential"],
        ),
        ("deadline", &["deadline", "due", "duedate", "cutoff", "target"]),
        ("reminder", &["reminder", "followup", "nudge", "ping", "checkin"]),
        (
            "attachment",
            &["attachment", "attached", "file", "document", "enclosed", "enclosure"],
        ),
        ("attached", &["attached", "attachment", "enclosed", "file", "included"]),
        ("document", &["document", "doc", "file", "paper", "pdf", "attachment"]),
        ("file", &["file", "document", "attachment", "doc"]),
        ("pdf", &["pdf", "document", "file", "acrobat"]),
        (
            "spreadsheet",
            &["spreadsheet", "excel", "xlsx", "xls", "sheet", "csv"],
        ),
        (
            "presentation",
            &["presentation", "ppt", "powerpoint", "slides", "deck"],
        ),
        ("report", &["report", "summary", "analysis", "review", "findings"]),
        (
            "invoice",
            &["invoice", "bill", "payment", "receipt", "statement", "billing"],
        ),
        (
            "payment",
            &[
                "payment",
                "pay",
                "invoice",
                "remittance",
                "transfer",
                "transaction",
            ],
        ),
        ("bill", &["bill", "invoice", "payment", "charge", "fee"]),
        ("receipt", &["receipt", "invoice", "confirmation", "proof"]),
        (
            "quote",
            &["quote", "quotation", "estimate", "proposal", "pricing", "bid"],
        ),
        ("budget", &["budget", "cost", "expense", "spending", "allocation"]),
        (
            "expense",
            &["expense", "cost", "spending", "reimbursement", "claim"],
        ),
        (
            "approve",
            &["approve", "approved", "approval", "authorize", "confirm", "signoff"],
        ),
        (
            "review",
            &["review", "feedback", "comments", "evaluate", "assess", "check"],
        ),
        ("feedback", &["feedback", "review", "comments", "input", "thoughts", "opinion"]),
        (
            "confirm",
            &["confirm", "confirmation", "verified", "acknowledge", "approve"],
        ),
        ("reject", &["reject", "rejected", "decline", "deny", "disapprove"]),
        ("update", &["update", "status", "progress", "news", "latest"]),
        ("status", &["status", "update", "progress", "state", "situation"]),
        (
            "progress",
            &["progress", "update", "status", "advancement", "development"],
        ),
        ("complete", &["complete", "completed", "done", "finished", "ready"]),
        ("pending", &["pending", "waiting", "outstanding", "incomplete", "open"]),
        ("team", &["team", "group", "squad", "crew", "staff"]),
        ("manager", &["manager", "supervisor", "boss", "lead", "director"]),
        ("client", &["client", "customer", "account", "partner"]),
        ("customer", &["customer", "client", "user", "buyer"]),
        ("vendor", &["vendor", "supplier", "provider", "contractor"]),
        ("send", &["send", "sent", "forward", "share", "transmit"]),
        ("forward", &["forward", "fwd", "send", "share", "pass"]),
        ("reply", &["reply", "respond", "answer", "response"]),
        ("request", &["request", "ask", "require", "need", "inquiry"]),
        ("submit", &["submit", "submitted", "send", "file", "deliver"]),
        ("share", &["share", "send", "forward", "distribute", "circulate"]),
        ("question", &["question", "query", "inquiry", "ask", "help"]),
        ("help", &["help", "assist", "support", "aid", "guidance"]),
        ("issue", &["issue", "problem", "bug", "error", "trouble", "concern"]),
        ("problem", &["problem", "issue", "bug", "error", "trouble"]),
        ("error", &["error", "bug", "issue", "problem", "mistake", "failure"]),
        ("today", &["today", "now"]),
        ("tomorrow", &["tomorrow"]),
        ("week", &["week", "weekly"]),
        ("month", &["month", "monthly"]),
        ("project", &["project", "initiative", "program", "effort", "work"]),
        ("task", &["task", "todo", "action", "item", "job", "assignment"]),
        (
            "milestone",
            &["milestone", "deliverable", "checkpoint", "goal", "target"],
        ),
        (
            "launch",
            &["launch", "release", "deploy", "ship", "rollout", "golive"],
        ),
        ("contract", &["contract", "agreement", "deal", "terms", "nda"]),
        (
            "agreement",
            &["agreement", "contract", "deal", "terms", "arrangement"],
        ),
        ("sign", &["sign", "signature", "execute", "approve", "authorize"]),
        ("legal", &["legal", "law", "attorney", "lawyer", "counsel", "compliance"]),
        ("travel", &["travel", "trip", "flight", "booking", "itinerary"]),
        ("flight", &["flight", "travel", "airline", "plane", "booking"]),
        ("hotel", &["hotel", "accommodation", "lodging", "booking", "stay"]),
        ("booking", &["booking", "reservation", "travel", "flight", "hotel"]),
    ]
}

#[derive(Clone)]
pub struct SynonymLookup {
    map: HashMap<String, BTreeSet<String>>,
}

impl SynonymLookup {
    pub fn new() -> Self {
        let mut map: HashMap<String, BTreeSet<String>> = HashMap::new();

        for (_canonical, group) in email_synonyms() {
            let normalized_group: BTreeSet<String> = group.iter().map(|s| s.to_lowercase()).collect();
            for w in group.iter() {
                let key = w.to_lowercase();
                if !map.contains_key(&key) {
                    map.insert(key, normalized_group.clone());
                }
            }
        }

        Self { map }
    }

    pub fn expand(&self, word: &str) -> String {
        let key = word.to_lowercase();
        if let Some(group) = self.map.get(&key) {
            if group.len() > 1 {
                let joined = group.iter().cloned().collect::<Vec<_>>().join(" OR ");
                return format!("({joined})");
            }
        }
        word.to_string()
    }
}


