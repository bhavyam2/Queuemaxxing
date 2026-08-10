"use strict";

let selected = null;
const held = new Map(); // receipt -> delivery

const el = (id) => document.getElementById(id);

function logLine(text, isError) {
  const li = document.createElement("li");
  if (isError) li.className = "error";
  const time = document.createElement("span");
  time.className = "time";
  time.textContent = new Date().toLocaleTimeString();
  li.appendChild(time);
  li.appendChild(document.createTextNode(text));
  const list = el("log");
  list.insertBefore(li, list.firstChild);
  while (list.children.length > 200) list.removeChild(list.lastChild);
}

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  if (res.status === 204) return null;
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) throw new Error((data && data.error) || res.status + " " + res.statusText);
  return data;
}

function truncate(s, n) {
  return s.length > n ? s.slice(0, n) + "..." : s;
}

async function refreshQueues() {
  let queues;
  try {
    queues = await api("GET", "/queues");
  } catch (err) {
    logLine("list queues: " + err.message, true);
    return;
  }

  const rows = el("queue-rows");
  rows.textContent = "";
  el("no-queues").hidden = queues.length > 0;

  for (const q of queues) {
    const tr = document.createElement("tr");
    if (q.name === selected) tr.className = "selected";

    const nameCell = document.createElement("td");
    const pick = document.createElement("button");
    pick.className = "name-button";
    pick.textContent = q.name;
    pick.addEventListener("click", () => selectQueue(q.name, q.ordering));
    nameCell.appendChild(pick);
    tr.appendChild(nameCell);

    for (const value of [q.ordering, q.stats.ready, q.stats.delayed, q.stats.in_flight, q.stats.total_enqueued, q.stats.total_acked]) {
      const td = document.createElement("td");
      td.className = "num";
      td.textContent = value;
      tr.appendChild(td);
    }

    const actions = document.createElement("td");
    const del = document.createElement("button");
    del.className = "secondary";
    del.textContent = "Delete";
    del.addEventListener("click", () => deleteQueue(q.name));
    actions.appendChild(del);
    tr.appendChild(actions);

    rows.appendChild(tr);
  }

  if (selected && !queues.some((q) => q.name === selected)) {
    selected = null;
    el("selected-panel").hidden = true;
    held.clear();
    renderHeld();
  }
}

function selectQueue(name, ordering) {
  selected = name;
  el("selected-name").textContent = name;
  el("selected-ordering").textContent = ordering;
  el("selected-panel").hidden = false;
  held.clear();
  renderHeld();
  refreshQueues();
}

async function deleteQueue(name) {
  try {
    await api("DELETE", "/queues/" + encodeURIComponent(name));
    logLine("deleted queue " + name);
  } catch (err) {
    logLine("delete " + name + ": " + err.message, true);
  }
  refreshQueues();
}

function renderHeld() {
  const rows = el("held-rows");
  rows.textContent = "";
  const any = held.size > 0;
  el("held-table").hidden = !any;
  el("no-held").hidden = any;

  for (const [receipt, d] of held) {
    const tr = document.createElement("tr");
    for (const value of [d.message_id.slice(0, 8), d.sequence, d.priority, d.receive_count, truncate(JSON.stringify(d.body), 40)]) {
      const td = document.createElement("td");
      td.className = "mono";
      td.textContent = value;
      tr.appendChild(td);
    }

    const actions = document.createElement("td");
    const wrap = document.createElement("div");
    wrap.className = "actions";

    const ack = document.createElement("button");
    ack.textContent = "Ack";
    ack.addEventListener("click", () => settle("ack", receipt, {}));

    const nack = document.createElement("button");
    nack.className = "secondary";
    nack.textContent = "Nack";
    nack.addEventListener("click", () => settle("nack", receipt, {}));

    const nackDelay = document.createElement("button");
    nackDelay.className = "secondary";
    nackDelay.textContent = "Nack +5s";
    nackDelay.addEventListener("click", () => settle("nack", receipt, { delay_seconds: 5 }));

    wrap.append(ack, nack, nackDelay);
    actions.appendChild(wrap);
    tr.appendChild(actions);
    rows.appendChild(tr);
  }
}

async function settle(action, receipt, extra) {
  const payload = Object.assign({ receipt }, extra);
  try {
    await api("POST", "/queues/" + encodeURIComponent(selected) + "/" + action, payload);
    held.delete(receipt);
    renderHeld();
    logLine(action + " " + receipt.slice(0, 8) + (extra.delay_seconds ? " with " + extra.delay_seconds + "s delay" : ""));
  } catch (err) {
    // A receipt whose visibility window expired is rejected; drop it from the held list.
    held.delete(receipt);
    renderHeld();
    logLine(action + " " + receipt.slice(0, 8) + ": " + err.message, true);
  }
  refreshQueues();
}

el("create-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const name = el("new-name").value.trim();
  const ordering = el("new-ordering").value;
  try {
    await api("POST", "/queues", { name, ordering });
    logLine("created queue " + name + " (" + ordering + ")");
    el("new-name").value = "";
    selectQueue(name, ordering);
  } catch (err) {
    logLine("create queue: " + err.message, true);
  }
  refreshQueues();
});

el("send-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  let parsed;
  try {
    parsed = JSON.parse(el("send-body").value);
  } catch (err) {
    logLine("body must be valid JSON: " + err.message, true);
    return;
  }
  const payload = {
    body: parsed,
    priority: Number(el("send-priority").value) || 0,
    delay_seconds: Number(el("send-delay").value) || 0,
  };
  try {
    const res = await api("POST", "/queues/" + encodeURIComponent(selected) + "/messages", payload);
    logLine("sent " + res.message_id.slice(0, 8) + " priority " + payload.priority + " delay " + payload.delay_seconds + "s");
  } catch (err) {
    logLine("send: " + err.message, true);
  }
  refreshQueues();
});

el("receive-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const payload = {
    visibility_timeout_seconds: Number(el("recv-visibility").value) || 0,
    wait_seconds: Number(el("recv-wait").value) || 0,
  };
  try {
    const d = await api("POST", "/queues/" + encodeURIComponent(selected) + "/receive", payload);
    if (d === null) {
      logLine("receive: no message available");
    } else {
      held.set(d.receipt, d);
      renderHeld();
      logLine("received " + d.message_id.slice(0, 8) + " seq " + d.sequence + " attempt " + d.receive_count);
    }
  } catch (err) {
    logLine("receive: " + err.message, true);
  }
  refreshQueues();
});

refreshQueues();
setInterval(refreshQueues, 1000);
