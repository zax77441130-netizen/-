const state = { view: "dashboard", jobs: [], candidates: [], usage: null, platforms: [], user: null };
const providerTestResults = new Map();
const app = document.querySelector("#app");
const title = document.querySelector("#page-title");
const ZH_TEXT = {
  "Dashboard": "總覽",
  "Jobs": "職缺",
  "Posts": "貼文",
  "Candidates": "候選人",
  "Resumes": "履歷",
  "Inbox": "訊息匣",
  "Pipeline": "流程",
  "Flow": "業務流",
  "Matching": "媒合",
  "Interviews": "面試",
  "Analytics": "分析",
  "AI Center": "AI 中心",
  "Platforms": "平台",
  "Automation": "自動化",
  "Queue": "佇列",
  "System Health": "系統健康",
  "Privacy": "隱私",
  "Audit Log": "稽核紀錄",
  "Login": "登入",
  "Login required": "需要登入",
  "RecruitOps AI": "招募營運 AI",
  "Use an administrator-issued account. Demo users are created only when DEMO_MODE is explicitly enabled.": "請使用管理員核發的帳號。只有明確啟用 DEMO_MODE 時才會建立示範使用者。",
  "Email": "電子郵件",
  "Password": "密碼",
  "Role": "角色",
  "New candidates": "新候選人",
  "All-time candidates; date filtered analytics available below": "累計候選人；下方分析可依日期檢視",
  "Recruiting jobs": "招募中職缺",
  "Open recruiting jobs": "開放招募中的職缺",
  "Applications": "應徵",
  "From Application records": "來自應徵紀錄",
  "From Interview records": "來自面試紀錄",
  "AI monthly tokens": "AI 本月權杖",
  "Funnel": "漏斗",
  "AI Usage": "AI 使用量",
  "Exposure and impression metrics: no source data yet.": "曝光與瀏覽指標：目前尚無來源資料。",
  "New Job": "新增職缺",
  "Title": "標題",
  "Area": "地區",
  "Shift": "班別",
  "Salary": "薪資",
  "Status": "狀態",
  "Copy": "文案",
  "Duplicate": "複製",
  "Pause": "暫停",
  "Cancel": "取消",
  "Back": "返回",
  "Recruiting Copy": "招募文案",
  "Platform": "平台",
  "Retry": "重試",
  "Error": "錯誤",
  "Name": "姓名",
  "Region": "區域",
  "Source": "來源",
  "Parsed": "解析結果",
  "Parse and Create Candidate": "解析並建立候選人",
  "Conversations": "對話",
  "New Conversation": "新增對話",
  "Channel": "渠道",
  "AI Mode": "AI 模式",
  "Open": "開啟",
  "Messages": "訊息",
  "Select a conversation.": "請選擇一段對話。",
  "Score": "分數",
  "Signals": "訊號",
  "Business Flow State": "業務流程狀態",
  "Stage": "階段",
  "Revision": "版本",
  "Rule": "規則",
  "Last Event": "最後事件",
  "Evidence": "證據",
  "Create Application": "建立應徵",
  "Create": "建立",
  "Pre-screen": "初篩",
  "Interview": "面試",
  "History": "歷程",
  "Run Match": "執行媒合",
  "Schedule": "排程",
  "Schedule Interview": "安排面試",
  "Date": "日期",
  "Time": "時間",
  "Method": "方式",
  "Total": "總計",
  "Deduplicated": "已去重",
  "Scheduled": "已排程",
  "AI Models": "AI 模型",
  "New Provider": "新增供應商",
  "Provider": "供應商",
  "Type": "類型",
  "Model": "模型",
  "Base URL": "基礎網址",
  "Latency": "延遲",
  "Capabilities": "能力",
  "Task Routes": "任務路由",
  "Prompt Library": "提示庫",
  "Run Router Test": "執行路由測試",
  "Token / Cost": "權杖 / 成本",
  "Save Route": "儲存路由",
  "Platform Adapters": "平台介接器",
  "Adapter": "介接器",
  "Accounts": "帳號",
  "Config": "設定",
  "External Actions": "外部操作",
  "Action": "操作",
  "Object": "物件",
  "Result": "結果",
  "Required Questions": "必要問題",
  "Question": "問題",
  "Answer": "回答",
  "Automation Runs": "自動化執行紀錄",
  "Capability": "能力",
  "Queue depth": "佇列深度",
  "Manual review": "人工審核",
  "Outbox pending": "待發布 outbox",
  "DLQ records": "死信紀錄",
  "Queue Tasks": "佇列任務",
  "Lease": "租約",
  "Fence": "柵欄權杖",
  "Redis Metrics": "Redis 指標",
  "Outbox": "Outbox",
  "Redis DLQ Records": "Redis 死信紀錄",
  "shown of": "筆，總數",
  "Stream": "串流",
  "Reason": "原因",
  "Replay": "重送",
  "Run": "執行",
  "Service Health": "服務健康",
  "System Errors": "系統錯誤",
  "Service": "服務",
  "Feature": "功能",
  "Severity": "嚴重度",
  "Message": "訊息",
  "Do Not Contact": "不聯繫名單",
  "Recruitment Blacklist": "招募黑名單",
  "Phone": "電話",
  "Scope": "範圍",
  "Confirm": "確認",
  "Activate": "啟用",
  "Lift": "解除",
  "Add Recruitment Blacklist Rule": "新增招募黑名單規則",
  "Add Do Not Contact": "新增不聯繫紀錄",
  "Actor": "操作者",
  "Entity": "實體",
  "Details": "詳細資料",
  "Healthy": "健康",
  "Degraded": "降級",
  "Unavailable": "無法使用",
  "Idle": "閒置",
  "Running": "執行中",
  "Database fallback": "資料庫備援",
  "Not Configured": "未設定",
  "Configured": "已設定",
  "Connected": "已連線",
  "Manual Review Required": "需要人工審核",
  "Pending Send": "等待發送",
  "Internal Record": "內部紀錄",
  "Completed": "已完成",
  "Queued": "已排入佇列",
  "Active": "啟用中",
  "Open": "開啟",
  "Closed": "已關閉",
  "Talent Pool": "人才庫",
  "HR Review": "HR 審核",
  "Recruiting": "招募中",
  "Paused": "已暫停",
  "Pending": "待處理",
  "Retry": "重試",
  "Failed": "失敗",
  "Success": "成功",
  "Cost": "成本",
  "Location": "地點",
  "Hours": "時段",
  "Contact name": "聯絡人姓名",
  "Contact method": "聯絡方式",
  "Description": "描述",
  "Requirements": "條件",
  "Full-time": "全職",
  "Job": "職缺",
  "Candidate": "候選人",
  "Shift": "班別",
  "Resume Import": "履歷匯入",
  "Parse Resume": "解析履歷",
  "File name optional": "檔名，可留空",
  "Paste resume text here": "在此貼上履歷文字",
  "Match Suggestions": "媒合建議",
  "Suggest Match": "建議媒合",
  "AI analysis is advisory only. HR must confirm interview, hire, reject, and placement decisions.": "AI 分析僅供參考；面試、錄用、拒絕與任用決策都必須由 HR 確認。",
  "Provider Name": "供應商名稱",
  "Endpoint": "端點",
  "API Key (optional)": "API 金鑰，可留空",
  "Daily token budget": "每日權杖預算",
  "Task name": "任務名稱",
  "Local fallback only": "僅使用本地備援",
  "Fallback provider IDs, comma-separated": "備援供應商 ID，以逗號分隔",
  "Primary": "主要",
  "Fallbacks": "備援",
  "Budget / Capability": "預算 / 能力",
  "Needs operator action": "需要操作人員處理",
  "Redis Streams publish backlog": "Redis 串流待發布積壓",
  "Queued + Retry + Running": "已排入佇列 + 重試 + 執行中",
  "When a platform is Not Configured or unsupported, use manual review: generate content, HR sends it, then record the result.": "平台未設定或尚未支援時，改走人工審核：先產生內容，由 HR 發送，再紀錄結果。",
  "Not Configured and Manual Review Required mean the system will not pretend to publish or import messages automatically.": "未設定與需要人工審核代表系統不會假裝自動發布或匯入訊息。",
  "Export": "匯出",
  "Delete Data": "刪除資料",
  "Add Entry": "新增紀錄",
  "Add Rule": "新增規則",
  "Candidate Data Tools": "候選人資料工具",
  "No candidate": "無候選人",
  "No scope id": "無範圍 ID",
  "Phone optional": "電話，可留空",
  "Email optional": "電子郵件，可留空",
  "Reason": "原因",
  "Evidence optional": "證據，可留空",
  "candidate": "候選人",
  "job": "職缺",
  "global": "全域",
  "text": "文字",
  "vision": "視覺",
  "tools": "工具",
  "json": "JSON",
  "JSON schema": "JSON schema",
  "Local": "本地",
  "OpenAI-compatible / vLLM / LM Studio": "OpenAI 相容 / vLLM / LM Studio",
  "OpenAI-Compatible": "OpenAI 相容",
  "Ollama native": "Ollama 原生",
  "phone": "電話",
  "onsite": "現場",
  "online": "線上",
  "suggest": "建議",
  "auto": "自動",
  "manual": "手動",
  "paused": "已暫停",
  "facebook": "Facebook",
  "line": "LINE",
  "email": "電子郵件",
  "web": "網站",
  "send externally": "發送到外部平台",
  "Send": "發送",
  "Message": "訊息",
  "Conversation": "對話",
};
const ZH_KEYS = {
  status: "狀態",
  database: "資料庫",
  redis: "Redis",
  queue: "佇列",
  worker: "工作器",
  worker_config: "工作器設定",
  external_services: "外部服務",
  llm_providers: "LLM 供應商",
  details: "詳細資料",
  queue_backend: "佇列後端",
  worker_queues: "工作佇列",
  queue_lease_seconds: "佇列租約秒數",
  redis_consumer_group: "Redis 消費群組",
  redis_pending_idle_ms: "Redis pending 閒置毫秒",
  redis_dlq_max_deliveries: "Redis 死信最大投遞次數",
  ocr_engine: "OCR 引擎",
  ocr_languages: "OCR 語言",
  ocr_raster_dpi: "OCR 影像 DPI",
  ocr_timeout_seconds: "OCR 逾時秒數",
  retention_cleanup_interval_seconds: "保留清理間隔秒數",
  retention_cleanup_batch_size: "保留清理批次大小",
  last_success_at: "最後成功時間",
  last_failure_reason: "最後失敗原因",
  redis_error: "Redis 錯誤",
  狀態: "狀態",
  資料庫: "資料庫",
  佇列: "佇列",
  工作器: "工作器",
  工作器設定: "工作器設定",
  外部服務: "外部服務",
  詳細資料: "詳細資料",
};

function zhText(value) {
  const text = String(value ?? "");
  return ZH_TEXT[text] || text;
}

function localize(value) {
  if (typeof value === "string") return zhText(value);
  if (Array.isArray(value)) return value.map(localize);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [ZH_KEYS[key] || key, localize(item)]));
  }
  return value;
}

function field(object, ...names) {
  for (const name of names) {
    if (object && Object.prototype.hasOwnProperty.call(object, name)) return object[name];
  }
  return undefined;
}

let translateScheduled = false;
function translatePage(root = document.body) {
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  const nodes = [];
  while (walker.nextNode()) nodes.push(walker.currentNode);
  nodes.forEach((node) => {
    const translated = zhText(node.nodeValue.trim());
    if (translated !== node.nodeValue.trim()) node.nodeValue = node.nodeValue.replace(node.nodeValue.trim(), translated);
  });
  root.querySelectorAll?.("[placeholder], [title], [aria-label]").forEach((el) => {
    ["placeholder", "title", "aria-label"].forEach((attr) => {
      if (el.hasAttribute(attr)) el.setAttribute(attr, zhText(el.getAttribute(attr)));
    });
  });
}

if (typeof MutationObserver !== "undefined") {
  new MutationObserver(() => {
    if (translateScheduled) return;
    translateScheduled = true;
    requestAnimationFrame(() => {
      translateScheduled = false;
      translatePage();
    });
  }).observe(document.body, { childList: true, subtree: true, characterData: true });
}

function csrfHeader() {
  const token = document.cookie.split("; ").find((row) => row.startsWith("recruit_csrf="))?.split("=")[1];
  return token ? { "X-CSRF-Token": decodeURIComponent(token) } : {};
}

const api = (path, options = {}) =>
  fetch(`/api${path}`, {
    headers: { "Content-Type": "application/json", ...csrfHeader(), ...(options.headers || {}) },
    ...options,
  }).then(async (response) => {
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || response.statusText);
    }
    return response.json();
  });

function replayRequestKey(taskId, revision) {
  const storageKey = `replay:${taskId}:${revision}`;
  let key = window.localStorage.getItem(storageKey);
  if (!key) {
    const random = window.crypto?.randomUUID ? window.crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    key = `manual-replay:${taskId}:${revision}:${random}`;
    window.localStorage.setItem(storageKey, key);
  }
  return key;
}

async function optionalApi(path, fallback) {
  try {
    return await api(path);
  } catch (error) {
    if (String(error.message || "").includes("Insufficient permission")) {
      return fallback;
    }
    throw error;
  }
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
}

function jsonBlock(value) {
  return esc(JSON.stringify(localize(value), null, 2));
}

async function refresh() {
  try {
    state.user = await api("/auth/me");
  } catch (_error) {
    renderLogin();
    return;
  }
  const [health, jobs, candidates, usage, platforms] = await Promise.all([
    api("/health"),
    api("/jobs"),
    api("/candidates"),
    optionalApi("/usage", { today_tokens: 0, month_tokens: 0, estimated_cost: 0, by_provider: {}, status_counts: {}, usage_source: {}, restricted: true }),
    api("/platforms"),
  ]);
  state.jobs = jobs;
  state.candidates = candidates;
  state.usage = usage;
  state.platforms = platforms;
  document.querySelector("#system-status").textContent = `系統 ${zhText(field(health, "status", "狀態"))} | 資料庫 ${zhText(field(health, "database", "資料庫"))}`;
  render();
}

function render() {
  document.querySelectorAll("nav button").forEach((button) => button.classList.toggle("active", button.dataset.view === state.view));
  const map = { dashboard: "總覽", jobs: "職缺", posts: "貼文", candidates: "候選人", resumes: "履歷", inbox: "訊息匣", pipeline: "流程", flow: "業務流", matching: "媒合", interviews: "面試", analytics: "分析", ai: "AI 中心", platforms: "平台", automation: "自動化", queue: "佇列", health: "系統健康", privacy: "隱私", audit: "稽核紀錄" };
  title.textContent = map[state.view] || state.view;
  const renderers = { dashboard: renderDashboard, jobs: renderJobs, posts: renderPosts, candidates: renderCandidates, resumes: renderResumes, inbox: renderInbox, pipeline: renderPipeline, flow: renderFlow, matching: renderMatching, interviews: renderInterviews, analytics: renderAnalytics, ai: renderAI, platforms: renderPlatforms, automation: renderAutomation, queue: renderQueue, health: renderHealth, privacy: renderPrivacy, audit: renderAudit };
  return (renderers[state.view] || renderDashboard)();
}

function renderLogin() {
  title.textContent = "登入";
  document.querySelector("#system-status").textContent = "需要登入";
  app.innerHTML = `
    <section class="panel">
      <h3>招募營運 AI</h3>
      <p class="muted">請使用管理員核發的帳號。只有明確啟用 DEMO_MODE 時才會建立示範使用者。</p>
      <form id="login-form">
        <input name="email" autocomplete="username" placeholder="電子郵件" required />
        <input name="password" type="password" autocomplete="current-password" placeholder="密碼" required />
        <button class="primary">登入</button>
      </form>
    </section>`;
  document.querySelector("#login-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    await api("/auth/login", { method: "POST", body: JSON.stringify(Object.fromEntries(new FormData(event.target).entries())) });
    await refresh();
  });
}

function metric(label, value, hint) {
  return `<section class="panel metric"><span>${esc(zhText(label))}</span><strong>${esc(zhText(value))}</strong><span>${esc(zhText(hint))}</span></section>`;
}

async function renderDashboard() {
  const summary = await api("/analytics/summary");
  const recruiting = state.jobs.filter((job) => job.status === "Recruiting").length;
  app.innerHTML = `
    <div class="grid">
      ${metric("Role", state.user.role, state.user.email)}
      ${metric("New candidates", state.candidates.length, "All-time candidates; date filtered analytics available below")}
      ${metric("Recruiting jobs", recruiting, "Open recruiting jobs")}
      ${metric("Applications", summary.applications, "From Application records")}
      ${metric("Interviews", summary.interviews, "From Interview records")}
      ${metric("AI monthly tokens", state.usage.month_tokens, `成本 ${state.usage.estimated_cost}`)}
    </div>
    <div class="two">
      <section class="panel"><h3>Funnel</h3><pre>${jsonBlock(summary.funnel)}</pre><p class="muted">Exposure and impression metrics: no source data yet.</p></section>
      <section class="panel"><h3>AI Usage</h3><pre>${jsonBlock(state.usage)}</pre></section>
    </div>`;
}

function renderJobs() {
  app.innerHTML = `
    <section class="panel">
      <div class="toolbar"><h3>職缺</h3><button class="primary" onclick="showJobForm()">新增職缺</button></div>
      <table><tr><th>標題</th><th>地區</th><th>班別</th><th>薪資</th><th>狀態</th><th></th></tr>
      ${state.jobs.map((job) => `<tr><td>${esc(job.title)}</td><td>${esc(job.area)}</td><td>${esc(job.shift)}</td><td>${esc(job.salary_text)}</td><td><span class="badge">${esc(zhText(job.status))}</span></td><td><button class="secondary" onclick="copywrite(${Number(job.id)})">文案</button> <button class="secondary" onclick="duplicateJob(${Number(job.id)})">複製</button> <button class="secondary" onclick="pauseJob(${Number(job.id)})">暫停</button></td></tr>`).join("")}</table>
    </section>`;
}

async function duplicateJob(jobId) {
  await api(`/jobs/${jobId}/duplicate`, { method: "POST" });
  await refresh();
}

async function pauseJob(jobId) {
  await api(`/jobs/${jobId}`, { method: "PATCH", body: JSON.stringify({ status: "Paused" }) });
  await refresh();
}

function showJobForm() {
  app.innerHTML = `
    <section class="panel"><h3>新增職缺</h3>
      <form id="job-form">
        <input name="title" placeholder="標題" required />
        <input name="location" placeholder="地點" required />
        <input name="area" placeholder="地區" required />
        <input name="shift" placeholder="班別" required />
        <input name="hours" placeholder="時段" required />
        <input name="salary_text" placeholder="薪資" required />
        <input name="contact_name" placeholder="聯絡人姓名" required />
        <input name="contact_method" placeholder="聯絡方式" required />
        <textarea class="full" name="description" placeholder="描述" required></textarea>
        <textarea class="full" name="requirements" placeholder="條件" required></textarea>
        <p id="job-form-error" class="form-message bad full" role="alert" aria-live="assertive" hidden></p>
        <button class="primary" type="submit">儲存</button>
        <button type="button" class="secondary" onclick="state.view='jobs'; render()">取消</button>
      </form>
    </section>`;
  document.querySelector("#job-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.target;
    const errorMessage = form.querySelector("#job-form-error");
    const submitButton = form.querySelector('button[type="submit"]');
    errorMessage.hidden = true;
    submitButton.disabled = true;
    submitButton.textContent = "儲存中...";
    try {
      const data = Object.fromEntries(new FormData(form).entries());
      await api("/jobs", { method: "POST", body: JSON.stringify({ ...data, job_type: "Full-time", required_headcount: 1 }) });
      await refresh();
    } catch (error) {
      errorMessage.textContent = `儲存失敗：${error.message || "請稍後再試"}`;
      errorMessage.hidden = false;
    } finally {
      submitButton.disabled = false;
      submitButton.textContent = "儲存";
    }
  });
}

async function copywrite(jobId) {
  try {
    const copy = await api("/ai/copywriting", { method: "POST", body: JSON.stringify({ job_id: jobId }) });
    const channels = [["titles", "標題建議"], ["facebook_long", "Facebook 長文"],
      ["facebook_short", "Facebook 短文"], ["recruit518", "518"],
      ["chickpt", "小雞上工"], ["line", "LINE"], ["dm", "私訊回覆"]];
    const notice = String(copy.ai_notice || "產出內容請由 HR 審核後再使用。");
    app.innerHTML = `<section class="panel"><div class="toolbar"><h3>招募文案</h3><button class="secondary" id="copy-back">返回</button></div>
      <p class="muted" id="copy-notice"></p><div class="toolbar" id="copy-tabs" role="tablist" aria-label="文案平台"></div>
      <div role="tabpanel" id="copy-panel"><h4 id="copy-heading"></h4><textarea id="copy-content" class="full" rows="14" aria-label="文案內容，可編輯"></textarea></div>
      <div class="toolbar"><button class="primary" id="copy-button">複製文案</button><span id="copy-feedback" role="status" aria-live="polite"></span></div></section>`;
    document.querySelector("#copy-notice").textContent = notice.includes("Template Mode")
      ? "內建模板產出，未使用外部 AI；發布前請由 HR 審核。" : notice;
    document.querySelector("#copy-back").onclick = () => { state.view = "jobs"; render(); };
    const content = document.querySelector("#copy-content");
    const drafts = {};
    channels.forEach(([key]) => { drafts[key] = Array.isArray(copy[key]) ? copy[key].join("\n") : String(copy[key] || ""); });
    let active = null;
    const buttons = [];
    function selectChannel(key, label) {
      if (active) drafts[active] = content.value;
      active = key;
      content.value = drafts[key];
      document.querySelector("#copy-heading").textContent = label;
      document.querySelector("#copy-feedback").textContent = "";
      buttons.forEach(([button, buttonKey]) => {
        button.setAttribute("aria-selected", String(buttonKey === key));
        button.className = buttonKey === key ? "primary" : "secondary";
      });
    }
    channels.forEach(([key, label]) => {
      const button = document.createElement("button");
      button.type = "button"; button.textContent = label; button.setAttribute("role", "tab");
      button.setAttribute("aria-controls", "copy-panel");
      button.onclick = () => selectChannel(key, label);
      buttons.push([button, key]); document.querySelector("#copy-tabs").append(button);
    });
    selectChannel("facebook_long", "Facebook 長文");
    document.querySelector("#copy-button").onclick = async () => {
      const feedback = document.querySelector("#copy-feedback");
      try {
        if (!navigator.clipboard?.writeText) throw new Error("clipboard unavailable");
        await navigator.clipboard.writeText(content.value);
        feedback.textContent = "已複製";
      } catch (_) {
        content.focus(); content.select();
        feedback.textContent = "無法自動複製，已選取文案，請按 Ctrl+C（Mac：⌘C）。";
      }
    };
  } catch (error) {
    app.innerHTML = `<section class="panel"><h3>文案產生失敗</h3><p class="bad">${esc(error.message)}</p><button class="secondary" onclick="state.view='jobs'; render()">返回職缺</button></section>`;
  }
}

async function renderPosts() {
  const posts = await api("/posts");
  app.innerHTML = `<section class="panel"><h3>Posts</h3><table><tr><th>Job</th><th>Platform</th><th>Status</th><th>Error</th><th>Retry</th></tr>${posts.map((post) => `<tr><td>${esc(post.job_id)}</td><td>${esc(post.platform)}</td><td>${esc(post.status)}</td><td>${esc(post.error_reason)}</td><td>${esc(post.retry_count)}</td></tr>`).join("")}</table><p class="muted">When a platform is Not Configured or unsupported, use manual review: generate content, HR sends it, then record the result.</p></section>`;
}

function renderCandidates() {
  app.innerHTML = `<section class="panel"><h3>Candidates</h3><table><tr><th>Name</th><th>Region</th><th>Shift</th><th>Source</th><th>Status</th><th></th></tr>${state.candidates.map((c) => `<tr><td>${esc(c.name)}</td><td>${esc(c.region)}</td><td>${esc(c.preferred_shift)}</td><td>${esc(c.source_platform)}</td><td><span class="badge">${esc(c.status)}</span></td><td><button class="secondary" onclick="markCandidate(${Number(c.id)}, 'HR Review')">HR Review</button><button class="secondary" onclick="markCandidate(${Number(c.id)}, 'Talent Pool')">Talent Pool</button></td></tr>`).join("")}</table></section>`;
}

async function renderResumes() {
  const resumes = await api("/resumes");
  app.innerHTML = `
    <section class="panel">
      <div class="toolbar"><h3>Resume Import</h3><button class="primary" onclick="showResumeForm()">Parse Resume</button></div>
      <table><tr><th>ID</th><th>Candidate</th><th>Source</th><th>Status</th><th>Parsed</th></tr>${resumes.map((resume) => `<tr><td>${Number(resume.id)}</td><td>${esc(resume.candidate_id)}</td><td>${esc(resume.source)}</td><td>${esc(resume.status)}</td><td><pre>${jsonBlock(resume.parsed_json)}</pre></td></tr>`).join("")}</table>
    </section>`;
}

function showResumeForm() {
  app.innerHTML = `
    <section class="panel"><h3>Parse Resume</h3>
      <form id="resume-form">
        <input name="source" value="manual" />
        <input name="file_name" placeholder="File name optional" />
        <textarea class="full" name="raw_text" rows="12" placeholder="Paste resume text here" required></textarea>
        <button class="primary">Parse and Create Candidate</button>
        <button type="button" class="secondary" onclick="state.view='resumes'; renderResumes()">Cancel</button>
      </form>
      <pre id="resume-result"></pre>
    </section>`;
  document.querySelector("#resume-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(event.target).entries());
    data.create_candidate = true;
    const result = await api("/resumes/parse", { method: "POST", body: JSON.stringify(data) });
    document.querySelector("#resume-result").textContent = JSON.stringify(result, null, 2);
    await refresh();
    state.view = "resumes";
  });
}

async function markCandidate(candidateId, status) {
  await api(`/candidates/${candidateId}`, { method: "PATCH", body: JSON.stringify({ status }) });
  await refresh();
}

async function renderInbox() {
  const conversations = await api("/conversations");
  app.innerHTML = `
    <div class="two">
      <section class="panel">
        <div class="toolbar"><h3>Conversations</h3><button class="primary" onclick="showConversationForm()">New Conversation</button></div>
        <table><tr><th>ID</th><th>Channel</th><th>AI Mode</th><th>Status</th><th></th></tr>${conversations.map((c) => `<tr><td>${Number(c.id)}</td><td>${esc(c.channel)}</td><td>${esc(c.ai_mode)}</td><td>${esc(c.status)}</td><td><button class="secondary" onclick="openConversation(${Number(c.id)})">Open</button></td></tr>`).join("")}</table>
      </section>
      <section class="panel" id="message-panel"><h3>Messages</h3><p class="muted">Select a conversation.</p></section>
    </div>`;
}

async function renderMatching() {
  const matches = await api("/matches");
  app.innerHTML = `
    <section class="panel">
      <div class="toolbar"><h3>Match Suggestions</h3><button class="primary" onclick="showMatchForm()">Suggest Match</button></div>
      <table><tr><th>ID</th><th>Candidate</th><th>Job</th><th>Score</th><th>Status</th><th>Signals</th></tr>${matches.map((m) => `<tr><td>${Number(m.id)}</td><td>${esc(m.candidate_id)}</td><td>${esc(m.job_id)}</td><td>${esc(m.score)}</td><td>${esc(m.status)}</td><td><pre>${jsonBlock(m.signals)}</pre></td></tr>`).join("")}</table>
      <p class="muted">AI analysis is advisory only. HR must confirm interview, hire, reject, and placement decisions.</p>
    </section>`;
}

async function renderFlow() {
  const flows = await api("/flow/states");
  app.innerHTML = `
    <section class="panel">
      <h3>Business Flow State</h3>
      <table><tr><th>Candidate</th><th>Job</th><th>Stage</th><th>Status</th><th>Revision</th><th>Rule</th><th>Last Event</th><th>Evidence</th></tr>${flows.map((flow) => `<tr><td>${esc(flow.candidate_id)}</td><td>${esc(flow.job_id)}</td><td><span class="badge">${esc(flow.stage)}</span></td><td>${esc(flow.status)}</td><td>${esc(flow.revision)}</td><td>${esc(flow.rule_version)}</td><td>${esc(flow.last_event_key)}</td><td><pre>${jsonBlock(flow.evidence)}</pre></td></tr>`).join("")}</table>
    </section>`;
}

async function renderPipeline() {
  const applications = await api("/applications");
  app.innerHTML = `
    <section class="panel">
      <div class="toolbar"><h3>Applications</h3><button class="primary" onclick="showApplicationForm()">Create Application</button></div>
      <table><tr><th>ID</th><th>Candidate</th><th>Job</th><th>Status</th><th>AI Score</th><th></th></tr>${applications.map((a) => `<tr><td>${Number(a.id)}</td><td>${esc(a.candidate_id)}</td><td>${esc(a.job_id)}</td><td><span class="badge">${esc(a.status)}</span></td><td>${esc(a.ai_match_score)}</td><td><button class="secondary" onclick="setApplicationStatus(${Number(a.id)}, 'Pre-screen')">Pre-screen</button><button class="secondary" onclick="setApplicationStatus(${Number(a.id)}, 'Interview')">Interview</button><button class="secondary" onclick="showApplicationHistory(${Number(a.id)})">History</button></td></tr>`).join("")}</table>
      <pre id="application-history"></pre>
    </section>`;
}

function showApplicationForm() {
  app.innerHTML = `
    <section class="panel"><h3>Create Application</h3>
      <form id="application-form">
        <select name="candidate_id">${state.candidates.map((c) => `<option value="${Number(c.id)}">${Number(c.id)} - ${esc(c.name)}</option>`).join("")}</select>
        <select name="job_id">${state.jobs.map((j) => `<option value="${Number(j.id)}">${Number(j.id)} - ${esc(j.title)}</option>`).join("")}</select>
        <button class="primary">Create</button>
        <button type="button" class="secondary" onclick="state.view='pipeline'; renderPipeline()">Cancel</button>
      </form>
    </section>`;
  document.querySelector("#application-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(event.target).entries());
    data.candidate_id = Number(data.candidate_id);
    data.job_id = Number(data.job_id);
    await api("/applications", { method: "POST", body: JSON.stringify(data) });
    state.view = "pipeline";
    await renderPipeline();
  });
}

async function setApplicationStatus(applicationId, status) {
  await api(`/applications/${applicationId}/status`, { method: "PATCH", body: JSON.stringify({ status, reason: "Updated from dashboard" }) });
  await renderPipeline();
}

async function showApplicationHistory(applicationId) {
  const history = await api(`/applications/${applicationId}/history`);
  document.querySelector("#application-history").textContent = JSON.stringify(history, null, 2);
}

function showMatchForm() {
  app.innerHTML = `
    <section class="panel"><h3>Suggest Match</h3>
      <form id="match-form">
        <select name="candidate_id">${state.candidates.map((c) => `<option value="${Number(c.id)}">${Number(c.id)} - ${esc(c.name)}</option>`).join("")}</select>
        <select name="job_id">${state.jobs.map((j) => `<option value="${Number(j.id)}">${Number(j.id)} - ${esc(j.title)}</option>`).join("")}</select>
        <button class="primary">Run Match</button>
        <button type="button" class="secondary" onclick="state.view='matching'; renderMatching()">Cancel</button>
      </form>
    </section>`;
  document.querySelector("#match-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(event.target).entries());
    data.candidate_id = Number(data.candidate_id);
    data.job_id = Number(data.job_id);
    await api("/matches/suggest", { method: "POST", body: JSON.stringify(data) });
    state.view = "matching";
    await renderMatching();
  });
}

async function renderInterviews() {
  const interviews = await api("/interviews");
  app.innerHTML = `
    <section class="panel">
      <div class="toolbar"><h3>Interviews</h3><button class="primary" onclick="showInterviewForm()">Schedule</button></div>
      <table><tr><th>ID</th><th>Candidate</th><th>Job</th><th>Date</th><th>Time</th><th>Method</th><th>Status</th></tr>${interviews.map((i) => `<tr><td>${Number(i.id)}</td><td>${esc(i.candidate_id)}</td><td>${esc(i.job_id)}</td><td>${esc(i.interview_date)}</td><td>${esc(i.interview_time)}</td><td>${esc(i.method)}</td><td>${esc(i.status)}</td></tr>`).join("")}</table>
    </section>`;
}

function showInterviewForm() {
  app.innerHTML = `
    <section class="panel"><h3>Schedule Interview</h3>
      <form id="interview-form">
        <select name="candidate_id">${state.candidates.map((c) => `<option value="${Number(c.id)}">${Number(c.id)} - ${esc(c.name)}</option>`).join("")}</select>
        <select name="job_id">${state.jobs.map((j) => `<option value="${Number(j.id)}">${Number(j.id)} - ${esc(j.title)}</option>`).join("")}</select>
        <input name="interview_date" type="date" required />
        <input name="interview_time" type="time" required />
        <select name="method"><option>phone</option><option>onsite</option><option>online</option></select>
        <input name="location" placeholder="Location optional" />
        <textarea class="full" name="notes" placeholder="Notes"></textarea>
        <button class="primary">Schedule</button>
        <button type="button" class="secondary" onclick="state.view='interviews'; renderInterviews()">Cancel</button>
      </form>
    </section>`;
  document.querySelector("#interview-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(event.target).entries());
    data.candidate_id = Number(data.candidate_id);
    data.job_id = Number(data.job_id);
    await api("/interviews", { method: "POST", body: JSON.stringify(data) });
    state.view = "interviews";
    await renderInterviews();
  });
}

async function renderAnalytics() {
  const summary = await api("/analytics/summary");
  app.innerHTML = `
    <div class="grid">
      ${metric("Jobs", summary.jobs, "Total")}
      ${metric("Candidates", summary.candidates, "Deduplicated")}
      ${metric("Applications", summary.applications, "Pipeline")}
      ${metric("Interviews", summary.interviews, "Scheduled")}
    </div>
    <div class="two">
      <section class="panel"><h3>Funnel</h3><pre>${jsonBlock(summary.funnel)}</pre></section>
      <section class="panel"><h3>Platform / Area</h3><pre>${jsonBlock({ by_platform: summary.by_platform, by_area: summary.by_area })}</pre></section>
    </div>`;
}

function showConversationForm() {
  app.innerHTML = `
    <section class="panel"><h3>New Conversation</h3>
      <form id="conversation-form">
        <select name="channel"><option>facebook</option><option>line</option><option>email</option><option>518</option><option>chickpt</option><option>web</option></select>
        <select name="ai_mode"><option>suggest</option><option>auto</option><option>manual</option><option>paused</option></select>
        <input name="candidate_id" placeholder="Candidate ID optional" />
        <input name="job_id" placeholder="Job ID optional" />
        <button class="primary">Save</button>
        <button type="button" class="secondary" onclick="state.view='inbox'; renderInbox()">Cancel</button>
      </form>
    </section>`;
  document.querySelector("#conversation-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(event.target).entries());
    ["candidate_id", "job_id"].forEach((key) => { data[key] = data[key] ? Number(data[key]) : null; });
    await api("/conversations", { method: "POST", body: JSON.stringify(data) });
    state.view = "inbox";
    await renderInbox();
  });
}

async function openConversation(conversationId) {
  const messages = await api(`/messages?conversation_id=${conversationId}`);
  document.querySelector("#message-panel").innerHTML = `<h3>Conversation #${Number(conversationId)}</h3><pre>${esc(messages.map((m) => `${m.sender_type}: ${m.body} [${m.delivery_status}]`).join("\n"))}</pre><form id="message-form"><input name="body" placeholder="Message" /><label class="inline"><input type="checkbox" name="send_external" /> send externally</label><button class="primary">Send</button></form>`;
  document.querySelector("#message-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.target);
    await api("/messages", { method: "POST", body: JSON.stringify({ conversation_id: conversationId, sender_type: "hr", body: form.get("body"), send_external: form.get("send_external") === "on" }) });
    await openConversation(conversationId);
  });
}

async function renderAI() {
  const [providers, routes, prompts] = await Promise.all([api("/ai/providers"), api("/ai/routes"), api("/prompts")]);
  app.innerHTML = `<section class="panel"><div class="toolbar"><h3>AI Models</h3><button class="primary" onclick="showProviderForm()">New Provider</button></div><table><tr><th>Provider</th><th>Model</th><th>Base URL</th><th>Status</th><th>Latency</th><th></th></tr>${providers.map((p) => `<tr><td>${esc(p.name)}</td><td>${esc(p.model)}</td><td>${esc(p.base_url)}</td><td>${esc(p.last_status)}</td><td>${esc(p.last_latency_ms || "-")}</td><td><button class="secondary" onclick="testProvider(${Number(p.id)})">Test</button></td></tr>`).join("")}</table><h3>Task Routes</h3><pre>${jsonBlock(routes)}</pre><h3>Prompt Library</h3><pre>${jsonBlock(prompts)}</pre><h3>Run Router Test</h3><form id="ai-run-form"><input name="task_name" value="MessageClassification" /><input name="prompt" value="薪資多少？" /><button class="primary">Run</button></form><pre id="ai-run-result"></pre><h3>Token / Cost</h3><pre>${jsonBlock(state.usage)}</pre></section>`;
  document.querySelector("#ai-run-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const result = await api("/ai/run", { method: "POST", body: JSON.stringify(Object.fromEntries(new FormData(event.target).entries())) });
    document.querySelector("#ai-run-result").textContent = JSON.stringify(result, null, 2);
    await refresh();
    state.view = "ai";
  });
}

function showProviderForm() {
  app.innerHTML = `
    <section class="panel"><h3>New AI Provider</h3>
      <form id="provider-form">
        <input name="name" placeholder="Provider Name" required />
        <select name="provider_type"><option>OpenAI-Compatible</option><option>Ollama</option><option>OpenAI</option><option>Anthropic</option><option>Gemini</option></select>
        <input name="base_url" placeholder="Base URL" value="http://localhost:11434/v1" required />
        <input name="endpoint" placeholder="Endpoint" value="/chat/completions" required />
        <input name="model" placeholder="Model" value="qwen3" required />
        <input name="api_key" placeholder="API Key (optional)" />
        <button class="primary">Save</button>
        <button type="button" class="secondary" onclick="state.view='ai'; renderAI()">Cancel</button>
      </form>
    </section>`;
  document.querySelector("#provider-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    await api("/ai/providers", { method: "POST", body: JSON.stringify(Object.fromEntries(new FormData(event.target).entries())) });
    await refresh();
    state.view = "ai";
    await renderAI();
  });
}

async function renderAI() {
  const [providers, routes, prompts] = await Promise.all([api("/ai/providers"), api("/ai/routes"), api("/prompts")]);
  const providerOptions = providers.map((p) => `<option value="${Number(p.id)}">${esc(p.name)} / ${esc(p.model)}</option>`).join("");
  app.innerHTML = `<section class="panel"><div class="toolbar"><h3>AI Models</h3><button class="primary" onclick="showProviderForm()">New Provider</button></div><table><tr><th>Provider</th><th>Type</th><th>Model</th><th>Capabilities</th><th>Status</th><th>Latency</th><th></th></tr>${providers.map((p) => { const test = providerTestResults.get(Number(p.id)); return `<tr><td>${esc(p.name)}</td><td>${esc(p.provider_type)}</td><td>${esc(p.model)}</td><td>${["text", p.supports_vision && "vision", p.supports_json_schema && "json", p.supports_tools && "tools"].filter(Boolean).map(esc).join(", ")}</td><td>${esc(p.last_status)}</td><td>${esc(p.last_latency_ms || "-")}</td><td><button class="secondary" id="provider-test-${Number(p.id)}" onclick="testProvider(${Number(p.id)})">Test</button>${test ? `<div class="provider-test-result ${test.ok ? "ok" : "warn"}"><strong>${test.ok ? "測試成功" : "測試失敗"}</strong><br>HTTP ${esc(test.httpStatus ?? "N/A")} · ${esc(test.latency ?? "-")} ms<pre>${esc(test.message || "")}</pre></div>` : ""}</td></tr>`; }).join("")}</table><h3>Task Routes</h3><form id="route-form" class="compact-form"><input name="task_name" placeholder="Task name" value="RecruitCopywriting" required /><select name="primary_provider_id"><option value="">Local fallback only</option>${providerOptions}</select><input name="fallback_provider_ids" placeholder="Fallback provider IDs, comma-separated" /><label class="inline"><input type="checkbox" name="requires_vision" /> vision</label><label class="inline"><input type="checkbox" name="requires_tools" /> tools</label><label class="inline"><input type="checkbox" name="requires_json_schema" /> JSON schema</label><input name="daily_token_budget" type="number" min="1" placeholder="Daily token budget" /><button class="primary">Save Route</button></form><table><tr><th>Task</th><th>Primary</th><th>Fallbacks</th><th>Budget / Capability</th></tr>${routes.map((r) => `<tr><td>${esc(r.task_name)}</td><td>${esc(r.primary_provider_id || "Local")}</td><td>${esc((r.fallback_provider_ids || []).join(", "))}</td><td><pre>${jsonBlock(r.budget_policy)}</pre></td></tr>`).join("")}</table><h3>Prompt Library</h3><pre>${jsonBlock(prompts)}</pre><h3>Run Router Test</h3><form id="ai-run-form"><input name="task_name" value="MessageClassification" /><input name="prompt" value="請分類候選人問題" /><button class="primary">Run</button></form><pre id="ai-run-result"></pre><h3>Token / Cost</h3><pre>${jsonBlock(state.usage)}</pre></section>`;
  document.querySelector("#route-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.target);
    const budget = {};
    ["requires_vision", "requires_tools", "requires_json_schema"].forEach((key) => {
      if (form.get(key) === "on") budget[key] = true;
    });
    if (form.get("daily_token_budget")) budget.daily_token_budget = Number(form.get("daily_token_budget"));
    await api("/ai/routes", {
      method: "POST",
      body: JSON.stringify({
        task_name: form.get("task_name"),
        primary_provider_id: form.get("primary_provider_id") ? Number(form.get("primary_provider_id")) : null,
        fallback_provider_ids: String(form.get("fallback_provider_ids") || "").split(",").map((item) => item.trim()).filter(Boolean).map(Number),
        budget_policy: budget,
      }),
    });
    await refresh();
    state.view = "ai";
  });
  document.querySelector("#ai-run-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const result = await api("/ai/run", { method: "POST", body: JSON.stringify(Object.fromEntries(new FormData(event.target).entries())) });
    document.querySelector("#ai-run-result").textContent = JSON.stringify(result, null, 2);
    await refresh();
    state.view = "ai";
  });
}

function showProviderForm() {
  app.innerHTML = `
    <section class="panel"><h3>New AI Provider</h3>
      <form id="provider-form">
        <input name="name" placeholder="Provider Name" required />
        <select name="provider_type" id="provider-type"><option value="openai_compatible">OpenAI-compatible / vLLM / LM Studio</option><option value="ollama">Ollama native</option><option value="openai">OpenAI</option><option value="anthropic">Anthropic</option><option value="gemini">Gemini</option><option value="local_template">Local Template</option></select>
        <input name="base_url" id="provider-base-url" placeholder="Base URL" value="http://ollama:11434" required />
        <input name="endpoint" placeholder="Endpoint" value="/api/chat" required />
        <input name="model" placeholder="Model" value="qwen3" required />
        <input name="api_key" placeholder="API Key (optional)" />
        <label class="inline"><input type="checkbox" name="supports_text" checked /> text</label>
        <label class="inline"><input type="checkbox" name="supports_vision" /> vision</label>
        <label class="inline"><input type="checkbox" name="supports_json_schema" /> JSON schema</label>
        <label class="inline"><input type="checkbox" name="supports_tools" /> tools</label>
        <input name="timeout_seconds" type="number" min="1" value="30" />
        <button class="primary">Save</button>
        <button type="button" class="secondary" onclick="state.view='ai'; renderAI()">Cancel</button>
      </form>
    </section>`;
  const examples = {
    openai_compatible: ["http://lm-studio:1234/v1", "/chat/completions"],
    ollama: ["http://ollama:11434", "/api/chat"],
    openai: ["https://api.openai.com/v1", "/chat/completions"],
    anthropic: ["https://api.anthropic.com", "/v1/messages"],
    gemini: ["https://generativelanguage.googleapis.com", "/v1beta/models/gemini-1.5-flash:generateContent"],
    local_template: ["local", ""],
  };
  document.querySelector("#provider-type").addEventListener("change", (event) => {
    const [base, endpoint] = examples[event.target.value] || examples.openai_compatible;
    document.querySelector("#provider-base-url").value = base;
    document.querySelector("[name='endpoint']").value = endpoint;
  });
  document.querySelector("#provider-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(event.target);
    const payload = Object.fromEntries(form.entries());
    ["supports_text", "supports_vision", "supports_json_schema", "supports_tools"].forEach((key) => {
      payload[key] = form.get(key) === "on";
    });
    payload.timeout_seconds = Number(payload.timeout_seconds || 30);
    await api("/ai/providers", { method: "POST", body: JSON.stringify(payload) });
    await refresh();
    state.view = "ai";
    await renderAI();
  });
}

async function testProvider(id) {
  const button = document.querySelector(`#provider-test-${Number(id)}`);
  if (button) {
    button.disabled = true;
    button.textContent = "Testing...";
  }
  const result = await api(`/ai/providers/${id}/test`, { method: "POST" });
  providerTestResults.set(Number(id), {
    ok: result.last_status === "Connected",
    httpStatus: result.test_http_status,
    latency: result.last_latency_ms,
    message: result.test_message,
  });
  await refresh();
  state.view = "ai";
  await renderAI();
}

async function renderPlatforms() {
  const accounts = await api("/platform-accounts");
  app.innerHTML = `<section class="panel"><h3>Platform Adapters</h3><table><tr><th>Platform</th><th>Adapter</th><th>Status</th><th>Message</th></tr>${state.platforms.map((p) => `<tr><td>${esc(p.platform)}</td><td>${esc(p.adapter)}</td><td class="${p.status === "Not Configured" ? "warn" : "ok"}">${esc(p.status)}</td><td>${esc(p.message)}</td></tr>`).join("")}</table><h3>Accounts</h3><table><tr><th>Platform</th><th>Name</th><th>Status</th><th>Capabilities</th><th>Config</th></tr>${accounts.map((a) => `<tr><td>${esc(a.platform)}</td><td>${esc(a.display_name)}</td><td>${esc(a.status)}</td><td>${esc(a.capabilities.join(", "))}</td><td><pre>${jsonBlock(a.config)}</pre></td></tr>`).join("")}</table><p class="muted">Not Configured and Manual Review Required mean the system will not pretend to publish or import messages automatically.</p></section>`;
}

async function renderAutomation() {
  const [runs, actions, answers] = await Promise.all([optionalApi("/automation/runs", []), optionalApi("/automation/actions", []), api("/question-answers")]);
  app.innerHTML = `
    <div class="two">
      <section class="panel"><h3>External Actions</h3><table><tr><th>Time</th><th>Platform</th><th>Action</th><th>Status</th><th>Object</th><th>Result</th></tr>${actions.map((a) => `<tr><td>${esc(a.attempted_at)}</td><td>${esc(a.platform)}</td><td>${esc(a.action_type)}</td><td>${esc(a.status)}</td><td>${esc(a.platform_object_id)}</td><td>${esc(a.result)}</td></tr>`).join("")}</table></section>
      <section class="panel"><h3>Required Questions</h3><table><tr><th>Candidate</th><th>Job</th><th>Question</th><th>Status</th><th>Answer</th></tr>${answers.map((a) => `<tr><td>${Number(a.candidate_id)}</td><td>${Number(a.job_id)}</td><td>${esc(a.question_text)}</td><td>${esc(a.answer_status)}</td><td>${esc(a.answer_text)}</td></tr>`).join("")}</table></section>
    </div>
    <section class="panel"><h3>Automation Runs</h3><table><tr><th>ID</th><th>Platform</th><th>Capability</th><th>Status</th><th>Error</th></tr>${runs.map((r) => `<tr><td>${Number(r.id)}</td><td>${esc(r.platform)}</td><td>${esc(r.capability)}</td><td>${esc(r.status)}</td><td>${esc(r.error_reason)}</td></tr>`).join("")}</table></section>`;
}

async function renderQueue() {
  const [tasks, health, metrics, redisMetrics, outbox, dlq] = await Promise.all([api("/queue/tasks"), api("/health"), api("/operations/metrics"), api("/queue/redis/metrics"), api("/queue/outbox?limit=50"), api("/queue/dlq?limit=50")]);
  const dlqItems = Array.isArray(dlq.items) ? dlq.items : [];
  const dlqTotal = Number(dlq.total || 0);
  const pendingOutbox = field(health, "queue_outbox_pending", "待發布佇列數") || 0;
  app.innerHTML = `<div class="grid">${metric("Queue depth", metrics.queue_depth, "Queued + Retry + Running")}${metric("Manual review", metrics.manual_review_count, "Needs operator action")}${metric("Outbox pending", pendingOutbox, "Redis Streams publish backlog")}${metric("DLQ records", dlqTotal, redisMetrics.redis.status)}</div><section class="panel"><h3>佇列任務</h3><table><tr><th>ID</th><th>佇列</th><th>類型</th><th>狀態</th><th>租約</th><th>柵欄權杖</th><th>Redis</th><th>重試</th><th>錯誤</th><th></th></tr>${tasks.map((task) => `<tr><td>${Number(task.id)}</td><td>${esc(task.queue_name)}</td><td>${esc(task.task_type)}</td><td>${esc(zhText(task.status))}</td><td>${esc(task.lease_expires_at || "-")}</td><td>${esc(task.fencing_token || 0)}</td><td>${esc(task.redis_stream_id || "-")}</td><td>${esc(task.retry_count)}/${esc(task.max_retries)}</td><td>${esc(task.last_error)}</td><td><button class="secondary" onclick="runTask(${Number(task.id)})">執行</button><button class="secondary" onclick="replayTask(${Number(task.id)}, ${Number(task.delivery_revision || 1)})">重送</button></td></tr>`).join("")}</table></section><div class="two"><section class="panel"><h3>Redis 指標</h3><pre>${jsonBlock(redisMetrics)}</pre></section><section class="panel"><h3>Outbox</h3><table><tr><th>ID</th><th>任務</th><th>版次</th><th>串流</th><th>狀態</th><th>錯誤</th></tr>${outbox.map((row) => `<tr><td>${Number(row.id)}</td><td>${Number(row.queue_task_id)}</td><td>${Number(row.delivery_revision)}</td><td>${esc(row.stream_name)}</td><td>${esc(zhText(row.status))}</td><td>${esc(row.last_error)}</td></tr>`).join("")}</table></section></div><section class="panel"><h3>Redis 死信紀錄</h3><p class="muted">顯示 ${dlqItems.length} 筆，總數 ${dlqTotal}</p><table><tr><th>ID</th><th>串流</th><th>任務</th><th>Outbox</th><th>版次</th><th>狀態</th><th>原因</th><th></th></tr>${dlqItems.map((row) => `<tr><td>${Number(row.id)}</td><td>${esc(row.stream_name)}</td><td>${esc(row.queue_task_id || "-")}</td><td>${esc(row.outbox_id || "-")}</td><td>${esc(row.delivery_revision || "-")}</td><td>${esc(zhText(row.status))}</td><td>${esc(row.reason)}</td><td><button class="secondary" onclick="replayDlq(${Number(row.id)})">重送</button></td></tr>`).join("")}</table></section>`;
}

async function runTask(taskId) {
  await api(`/queue/tasks/${taskId}/run`, { method: "POST" });
  await renderQueue();
}

async function replayTask(taskId, revision) {
  const requestKey = replayRequestKey(taskId, revision);
  await api(`/queue/tasks/${taskId}/replay`, { method: "POST", body: JSON.stringify({ expected_delivery_revision: revision, replay_request_key: requestKey }) });
  await renderQueue();
}

async function replayDlq(recordId) {
  await api(`/queue/dlq/${recordId}/replay`, { method: "POST" });
  await renderQueue();
}

async function renderAudit() {
  const logs = await api("/audit-logs");
  app.innerHTML = `<section class="panel"><h3>Audit Log</h3><table><tr><th>ID</th><th>Actor</th><th>Action</th><th>Entity</th><th>Details</th></tr>${logs.map((log) => `<tr><td>${Number(log.id)}</td><td>${esc(log.actor_user_id)}</td><td>${esc(log.action)}</td><td>${esc(log.entity_type)} ${esc(log.entity_id)}</td><td><pre>${jsonBlock(log.details)}</pre></td></tr>`).join("")}</table></section>`;
}

async function renderHealth() {
  const [health, errors] = await Promise.all([api("/health"), optionalApi("/system/errors", [])]);
  app.innerHTML = `<div class="two"><section class="panel"><h3>Service Health</h3><pre>${jsonBlock(health)}</pre></section><section class="panel"><h3>System Errors</h3><table><tr><th>Service</th><th>Feature</th><th>Severity</th><th>Status</th><th>Message</th></tr>${errors.map((error) => `<tr><td>${esc(error.service)}</td><td>${esc(error.feature)}</td><td>${esc(error.severity)}</td><td>${esc(error.status)}</td><td>${esc(error.message)}</td></tr>`).join("")}</table></section></div>`;
}

async function renderPrivacy() {
  const [entries, blacklist] = await Promise.all([api("/do-not-contact"), optionalApi("/recruitment-blacklist", [])]);
  app.innerHTML = `
    <section class="panel">
      <div class="toolbar"><h3>Do Not Contact</h3><button class="primary" onclick="showDncForm()">Add Entry</button></div>
      <table><tr><th>ID</th><th>Candidate</th><th>Status</th><th>Reason</th><th></th></tr>${entries.map((entry) => `<tr><td>${Number(entry.id)}</td><td>${esc(entry.candidate_id)}</td><td>${esc(entry.status)}</td><td>${esc(entry.reason)}</td><td><button class="secondary" onclick="liftDnc(${Number(entry.id)})">Lift</button></td></tr>`).join("")}</table>
      <div class="toolbar"><h3>Recruitment Blacklist</h3><button class="primary" onclick="showBlacklistForm()">Add Rule</button></div>
      <table><tr><th>ID</th><th>Candidate</th><th>Phone</th><th>Email</th><th>Scope</th><th>Status</th><th>Reason</th><th></th></tr>${blacklist.map((entry) => `<tr><td>${Number(entry.id)}</td><td>${esc(entry.candidate_id || "-")}</td><td>${esc(entry.phone || "-")}</td><td>${esc(entry.email || "-")}</td><td>${esc(entry.scope_type)}:${esc(entry.scope_id || "-")}</td><td>${esc(entry.status)}</td><td>${esc(entry.reason)}</td><td><button class="secondary" onclick="confirmBlacklistScope(${Number(entry.id)})">Confirm</button><button class="secondary" onclick="activateBlacklist(${Number(entry.id)})">Activate</button><button class="secondary" onclick="liftBlacklist(${Number(entry.id)})">Lift</button></td></tr>`).join("")}</table>
      <h3>Candidate Data Tools</h3>
      <form id="privacy-form"><select name="candidate_id">${state.candidates.map((c) => `<option value="${Number(c.id)}">${Number(c.id)} - ${esc(c.name)}</option>`).join("")}</select><button class="secondary" name="action" value="export">Export</button><button class="secondary" name="action" value="delete">Delete Data</button></form>
      <pre id="privacy-result"></pre>
    </section>`;
  document.querySelector("#privacy-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const submitter = event.submitter;
    const candidateId = new FormData(event.target).get("candidate_id");
    if (submitter.value === "export") {
      const data = await api(`/candidates/${candidateId}/export`);
      document.querySelector("#privacy-result").textContent = JSON.stringify(data, null, 2);
    } else {
      const data = await api(`/candidates/${candidateId}`, { method: "DELETE" });
      document.querySelector("#privacy-result").textContent = JSON.stringify(data, null, 2);
      await refresh();
      state.view = "privacy";
    }
  });
}

async function confirmBlacklistScope(entryId) {
  const scopeType = window.prompt("Scope type: candidate, job, or global", "job");
  if (!scopeType) return;
  const scopeId = scopeType === "job" ? window.prompt("Job ID") : "";
  await api(`/recruitment-blacklist/${entryId}/confirm-scope`, { method: "POST", body: JSON.stringify({ scope_type: scopeType, scope_id: scopeId || null, note: "confirmed in operations UI" }) });
  await renderPrivacy();
}

async function activateBlacklist(entryId) {
  await api(`/recruitment-blacklist/${entryId}/activate`, { method: "POST" });
  await renderPrivacy();
}

async function liftBlacklist(entryId) {
  const reason = window.prompt("Lift reason", "resolved");
  if (!reason) return;
  await api(`/recruitment-blacklist/${entryId}/lift`, { method: "POST", body: JSON.stringify({ reason }) });
  await renderPrivacy();
}

function showBlacklistForm() {
  app.innerHTML = `
    <section class="panel"><h3>Add Recruitment Blacklist Rule</h3>
      <form id="blacklist-form">
        <select name="candidate_id"><option value="">No candidate</option>${state.candidates.map((c) => `<option value="${Number(c.id)}">${Number(c.id)} - ${esc(c.name)}</option>`).join("")}</select>
        <input name="phone" placeholder="Phone optional" />
        <input name="email" placeholder="Email optional" />
        <select name="scope_type"><option value="candidate">Candidate</option><option value="job">Job</option><option value="global">Global</option></select>
        <select name="scope_id"><option value="">No scope id</option>${state.jobs.map((j) => `<option value="${Number(j.id)}">Job ${Number(j.id)} - ${esc(j.title)}</option>`).join("")}</select>
        <input name="rule_code" value="customer_rule" />
        <textarea class="full" name="reason" placeholder="Reason" required></textarea>
        <textarea class="full" name="evidence" placeholder="Evidence optional"></textarea>
        <button class="primary">Add</button>
        <button type="button" class="secondary" onclick="state.view='privacy'; renderPrivacy()">Cancel</button>
      </form>
    </section>`;
  document.querySelector("#blacklist-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(event.target).entries());
    data.candidate_id = data.candidate_id ? Number(data.candidate_id) : null;
    data.scope_id = data.scope_id || null;
    if (!data.phone) data.phone = null;
    if (!data.email) data.email = null;
    if (!data.evidence) data.evidence = null;
    await api("/recruitment-blacklist", { method: "POST", body: JSON.stringify(data) });
    state.view = "privacy";
    await renderPrivacy();
  });
}

function showDncForm() {
  app.innerHTML = `
    <section class="panel"><h3>Add Do Not Contact</h3>
      <form id="dnc-form">
        <select name="candidate_id">${state.candidates.map((c) => `<option value="${Number(c.id)}">${Number(c.id)} - ${esc(c.name)}</option>`).join("")}</select>
        <textarea class="full" name="reason" placeholder="Reason" required></textarea>
        <textarea class="full" name="evidence" placeholder="Evidence optional"></textarea>
        <button class="primary">Add</button>
        <button type="button" class="secondary" onclick="state.view='privacy'; renderPrivacy()">Cancel</button>
      </form>
    </section>`;
  document.querySelector("#dnc-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(event.target).entries());
    data.candidate_id = Number(data.candidate_id);
    await api("/do-not-contact", { method: "POST", body: JSON.stringify(data) });
    state.view = "privacy";
    await renderPrivacy();
  });
}

async function liftDnc(entryId) {
  await api(`/do-not-contact/${entryId}/lift`, { method: "POST", body: JSON.stringify({ reason: "Lifted from dashboard" }) });
  await renderPrivacy();
}

document.querySelectorAll("nav button").forEach((button) => button.addEventListener("click", () => {
  state.view = button.dataset.view;
  render();
}));

refresh().catch((error) => {
  app.innerHTML = `<section class="panel"><p class="bad">${esc(error.message)}</p></section>`;
});
