let bases = [];
let blacklistRows = [];
const contactsPager = {};
const BASE_CONTACTS_PAGE_SIZE = 100;

document.addEventListener('DOMContentLoaded', async () => {
    setupHandlers();
    await Promise.all([loadBases(), loadBlacklist()]);
});

function setupHandlers() {
    document.getElementById('reloadBasesBtn')?.addEventListener('click', loadBases);
    document.getElementById('addBlacklistBtn')?.addEventListener('click', addBlacklistFromInput);
    document.getElementById('blacklistUserIdInput')?.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') addBlacklistFromInput();
    });
    document.getElementById('parseChatBtn')?.addEventListener('click', parseChatToBase);

    const uploadArea = document.getElementById('baseUploadArea');
    const fileInput = document.getElementById('baseFileInput');
    if (uploadArea && fileInput) {
        uploadArea.addEventListener('click', () => fileInput.click());
        uploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadArea.classList.add('dragover');
        });
        uploadArea.addEventListener('dragleave', () => uploadArea.classList.remove('dragover'));
        uploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadArea.classList.remove('dragover');
            const file = e.dataTransfer.files?.[0];
            if (file) uploadBaseFile(file);
        });
        fileInput.addEventListener('change', (e) => {
            const file = e.target.files?.[0];
            if (file) uploadBaseFile(file);
            fileInput.value = '';
        });
    }
}

async function loadBases() {
    const response = await fetch('/api/bases');
    const data = await response.json();
    bases = Array.isArray(data) ? data : [];
    renderBases();
}

function renderBases() {
    const wrap = document.getElementById('basesList');
    if (!wrap) return;
    if (!bases.length) {
        wrap.innerHTML = '<div class="empty-state">Баз пока нет</div>';
        return;
    }

    wrap.innerHTML = bases.map(base => {
        const baseId = Number(base.id);
        const c = Number(base.contacts_count || 0);
        const created = formatDate(base.created_at);
        return `
            <div class="card" style="margin-bottom: 10px;">
                <div class="panel-head">
                    <div>
                        <strong>${escapeHtml(base.name || `База #${baseId}`)}</strong>
                        <div class="muted-text">ID ${baseId} · контактов ${c} · загружена ${escapeHtml(created)}</div>
                    </div>
                    <div class="btn-group">
                        <button class="btn btn-small" data-action="toggle" data-id="${baseId}">Открыть</button>
                        <button class="btn btn-small btn-danger" data-action="delete" data-id="${baseId}">Удалить</button>
                    </div>
                </div>
                <div id="baseContactsWrap_${baseId}" style="display:none; margin-top: 10px;"></div>
            </div>
        `;
    }).join('');

    wrap.querySelectorAll('button[data-action="toggle"]').forEach(btn => {
        btn.addEventListener('click', async () => {
            const baseId = Number(btn.dataset.id);
            const panel = document.getElementById(`baseContactsWrap_${baseId}`);
            if (!panel) return;
            const hidden = panel.style.display === 'none';
            if (!hidden) {
                panel.style.display = 'none';
                btn.textContent = 'Открыть';
                return;
            }
            panel.style.display = 'block';
            btn.textContent = 'Скрыть';
            contactsPager[baseId] = contactsPager[baseId] || { page: 1 };
            await loadBaseContacts(baseId);
        });
    });

    wrap.querySelectorAll('button[data-action="delete"]').forEach(btn => {
        btn.addEventListener('click', async () => {
            const baseId = Number(btn.dataset.id);
            if (!Number.isFinite(baseId) || baseId <= 0) return;
            if (!confirm('Удалить базу?')) return;
            const response = await fetch(`/api/bases/${baseId}`, { method: 'DELETE' });
            const data = await response.json();
            if (!response.ok || data.error) {
                alert(data.error || 'Ошибка удаления');
                return;
            }
            await loadBases();
        });
    });
}

async function loadBaseContacts(baseId) {
    const pager = contactsPager[baseId] || { page: 1 };
    const offset = Math.max(0, (pager.page - 1) * BASE_CONTACTS_PAGE_SIZE);
    const response = await fetch(`/api/bases/${baseId}/contacts?limit=${BASE_CONTACTS_PAGE_SIZE}&offset=${offset}`);
    const data = await response.json();
    const items = Array.isArray(data.items) ? data.items : [];
    const total = Number(data.total || 0);
    renderBaseContacts(baseId, items, total);
}

function renderBaseContacts(baseId, items, total) {
    const panel = document.getElementById(`baseContactsWrap_${baseId}`);
    if (!panel) return;
    const pager = contactsPager[baseId] || { page: 1 };
    const pages = Math.max(1, Math.ceil(total / BASE_CONTACTS_PAGE_SIZE));
    if (pager.page > pages) pager.page = pages;

    const hasName = items.some(x => (x.name || '').trim().length > 0);
    const hasCompany = items.some(x => (x.company || '').trim().length > 0);
    const hasPosition = items.some(x => (x.position || '').trim().length > 0);
    const hasPhone = items.some(x => (x.phone || '').trim().length > 0);
    const hasUsername = items.some(x => (x.username || '').trim().length > 0);
    const hasUserId = items.some(x => Number(x.user_id || 0) > 0);

    const cols = [];
    if (hasUserId) cols.push({ key: 'user_id', title: 'user_id' });
    if (hasUsername) cols.push({ key: 'username', title: 'username' });
    if (hasPhone) cols.push({ key: 'phone', title: 'phone' });
    if (hasName) cols.push({ key: 'name', title: 'name' });
    if (hasCompany) cols.push({ key: 'company', title: 'company' });
    if (hasPosition) cols.push({ key: 'position', title: 'position' });

    if (!cols.length) {
        cols.push({ key: 'user_id', title: 'Контакт' });
    }

    const rowsHtml = items.length
        ? items.map(item => `
            <tr>
                ${cols.map(c => `<td>${escapeHtml(item[c.key] || '—')}</td>`).join('')}
            </tr>
        `).join('')
        : `<tr><td colspan="${cols.length}" style="text-align:center; padding: 16px;">Контактов нет</td></tr>`;

    const start = total > 0 ? ((pager.page - 1) * BASE_CONTACTS_PAGE_SIZE) + 1 : 0;
    const end = Math.min(pager.page * BASE_CONTACTS_PAGE_SIZE, total);

    panel.innerHTML = `
        <div class="table-container">
            <table>
                <thead>
                    <tr>${cols.map(c => `<th>${escapeHtml(c.title)}</th>`).join('')}</tr>
                </thead>
                <tbody>${rowsHtml}</tbody>
            </table>
        </div>
        <div class="btn-group" style="margin-top: 8px; gap: 8px;">
            <button type="button" class="btn btn-small" id="basePrev_${baseId}" ${pager.page <= 1 ? 'disabled' : ''}>← Назад</button>
            <small class="muted-text">Стр. ${pager.page}/${pages} · показано ${start}-${end} из ${total}</small>
            <button type="button" class="btn btn-small" id="baseNext_${baseId}" ${pager.page >= pages ? 'disabled' : ''}>Вперёд →</button>
        </div>
    `;

    document.getElementById(`basePrev_${baseId}`)?.addEventListener('click', async () => {
        if (pager.page <= 1) return;
        pager.page -= 1;
        contactsPager[baseId] = pager;
        await loadBaseContacts(baseId);
    });
    document.getElementById(`baseNext_${baseId}`)?.addEventListener('click', async () => {
        if (pager.page >= pages) return;
        pager.page += 1;
        contactsPager[baseId] = pager;
        await loadBaseContacts(baseId);
    });
}

async function uploadBaseFile(file) {
    const status = document.getElementById('baseUploadStatus');
    const baseName = (document.getElementById('baseNameInput')?.value || '').trim();
    if (!baseName) {
        status.textContent = 'Укажите название базы';
        return;
    }
    status.textContent = 'Загрузка...';
    const formData = new FormData();
    formData.append('file', file);
    formData.append('base_name', baseName);

    const response = await fetch('/api/bases/upload', { method: 'POST', body: formData });
    const data = await response.json();
    if (!response.ok || data.error) {
        status.textContent = data.error || 'Ошибка загрузки базы';
        return;
    }
    const v = data.validation || {};
    const validation = v.rows_total ? ` · валидно ${v.rows_valid}/${v.rows_total}` : '';
    status.textContent = `Готово: imported ${data.imported || 0}, skipped ${data.skipped || 0}${validation}`;
    document.getElementById('baseNameInput').value = '';
    await loadBases();
}

async function parseChatToBase() {
    const status = document.getElementById('parseChatStatus');
    const chat = (document.getElementById('parseChatInput')?.value || '').trim();
    const baseName = (document.getElementById('parseBaseNameInput')?.value || '').trim();
    const mode = (document.getElementById('parseModeInput')?.value || 'auto').trim();
    const messagesLimitRaw = Number(document.getElementById('parseMessagesLimitInput')?.value || 3000);
    const messagesLimit = Number.isFinite(messagesLimitRaw) ? Math.max(100, Math.min(10000, Math.floor(messagesLimitRaw))) : 3000;
    if (!chat) {
        status.textContent = 'Укажите ссылку/username чата';
        return;
    }
    if (!baseName) {
        status.textContent = 'Укажите название базы';
        return;
    }
    status.textContent = 'Парсинг...';
    const response = await fetch('/api/bases/parse-chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat, base_name: baseName, mode, messages_limit: messagesLimit })
    });
    const data = await response.json();
    if (!response.ok || data.error) {
        status.textContent = data.error || 'Ошибка парсинга';
        return;
    }
    status.textContent = `Готово: imported ${data.imported || 0}, skipped ${data.skipped || 0} · режим ${data.mode || mode}`;
    document.getElementById('parseChatInput').value = '';
    document.getElementById('parseBaseNameInput').value = '';
    await loadBases();
}

async function loadBlacklist() {
    const response = await fetch('/api/bases/blacklist');
    const rows = await response.json();
    blacklistRows = Array.isArray(rows) ? rows : [];
    renderBlacklist();
}

function renderBlacklist() {
    const wrap = document.getElementById('blacklistList');
    if (!wrap) return;
    if (!blacklistRows.length) {
        wrap.innerHTML = '<small class="muted-text">Черный список пуст</small>';
        return;
    }
    wrap.innerHTML = blacklistRows.map(row => `
        <span class="template-tag" style="display:inline-flex; align-items:center; gap:6px;">
            ${escapeHtml(String(row.user_id))}
            <button class="btn btn-small" data-action="remove-blacklist" data-user-id="${escapeHtml(String(row.user_id))}">×</button>
        </span>
    `).join('');
    wrap.querySelectorAll('button[data-action="remove-blacklist"]').forEach(btn => {
        btn.addEventListener('click', async () => {
            const uid = Number(btn.dataset.userId);
            if (!Number.isFinite(uid)) return;
            await removeFromBlacklist(uid);
        });
    });
}

async function addBlacklistFromInput() {
    const input = document.getElementById('blacklistUserIdInput');
    const uid = Number(input?.value || 0);
    if (!Number.isFinite(uid) || uid <= 0) return;
    const response = await fetch('/api/bases/blacklist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: uid })
    });
    const data = await response.json();
    if (!response.ok || data.error) {
        alert(data.error || 'Ошибка добавления');
        return;
    }
    input.value = '';
    await loadBlacklist();
}

async function removeFromBlacklist(userId) {
    const response = await fetch(`/api/bases/blacklist/${encodeURIComponent(userId)}`, { method: 'DELETE' });
    const data = await response.json();
    if (!response.ok || data.error) {
        alert(data.error || 'Ошибка удаления');
        return;
    }
    await loadBlacklist();
}

function formatDate(value) {
    if (!value) return '—';
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return value;
    return d.toLocaleString('ru-RU');
}

function escapeHtml(value) {
    return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
}
