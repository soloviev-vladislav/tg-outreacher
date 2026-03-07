let currentCampaignId = null;
let campaigns = [];
let accounts = [];
let selectedCampaignAccounts = new Set();
let templates = [];
let refreshTimer = null;
let bases = [];
let contactsPage = 1;
const CONTACTS_PAGE_SIZE = 100;

const SELECTED_CAMPAIGN_KEY = 'outreach_selected_campaign_id';

document.addEventListener('DOMContentLoaded', async () => {
    setupEventListeners();
    await Promise.all([loadAccounts(), loadTemplates(), loadBases(), loadCampaigns()]);
    restoreSelectedCampaign();
    startAutoRefresh();
});

function setupEventListeners() {
    document.getElementById('newCampaignBtn')?.addEventListener('click', newCampaign);
    document.getElementById('saveCampaignBtn')?.addEventListener('click', saveCampaign);
    document.getElementById('saveTemplateBtn')?.addEventListener('click', saveTemplate);
    document.getElementById('previewSpintaxBtn')?.addEventListener('click', previewSpintax);

    document.getElementById('startCampaignBtn')?.addEventListener('click', () => currentCampaignId && startCampaign(currentCampaignId));
    document.getElementById('pauseCampaignBtn')?.addEventListener('click', () => currentCampaignId && pauseCampaign(currentCampaignId));
    document.getElementById('stopCampaignBtn')?.addEventListener('click', () => currentCampaignId && stopCampaign(currentCampaignId));
    document.getElementById('deleteCampaignBtn')?.addEventListener('click', () => currentCampaignId && deleteCampaign(currentCampaignId));
    document.getElementById('selectAllCampaignAccountsBtn')?.addEventListener('click', selectAllCampaignAccounts);
    document.getElementById('clearCampaignAccountsBtn')?.addEventListener('click', clearCampaignAccounts);

    document.getElementById('addFollowupStepBtn')?.addEventListener('click', () => addFollowupStep());
    document.getElementById('campaignBaseId')?.addEventListener('change', () => {
        const baseIdRaw = document.getElementById('campaignBaseId')?.value || '';
        const baseId = Number(baseIdRaw);
        const selected = bases.find(b => b.id === baseId);
        const info = selected
            ? `База: ${selected.name} · контактов ${selected.contacts_count || 0}`
            : 'База не выбрана';
        showSaveStatus(info);
    });
    document.getElementById('exportStepReportCsvBtn')?.addEventListener('click', () => exportStepReport('csv'));
    document.getElementById('exportStepReportXlsxBtn')?.addEventListener('click', () => exportStepReport('xlsx'));
}

function startAutoRefresh() {
    if (refreshTimer) clearInterval(refreshTimer);
    refreshTimer = setInterval(async () => {
        await loadCampaigns();
        if (currentCampaignId) {
            const campaign = campaigns.find(c => c.id === currentCampaignId);
            if (campaign) {
                updateCampaignHeader(campaign);
                updateActionButtons(campaign.status);
            }
            await Promise.all([loadContacts(currentCampaignId), refreshReadiness(currentCampaignId)]);
        }
    }, 15000);
}

function showSaveStatus(text, isError = false) {
    const el = document.getElementById('campaignSaveStatus');
    if (!el) return;
    el.textContent = text;
    el.className = `save-status ${isError ? 'error' : 'ok'}`;
    setTimeout(() => {
        if (el.textContent === text) {
            el.textContent = '';
            el.className = 'save-status';
        }
    }, 2500);
}

function normalizeCampaignStatus(status) {
    if (!status) return 'draft';
    return String(status).toLowerCase();
}

function statusBadgeClass(status) {
    const st = normalizeCampaignStatus(status);
    if (st === 'active') return 'badge-success';
    if (st === 'paused') return 'badge-warning';
    if (st === 'stopped') return 'badge-danger';
    return 'badge';
}

async function loadAccounts() {
    const response = await fetch('/api/accounts/detailed');
    const all = await response.json();
    accounts = (all || []).filter(a => a.account_type === 'outreach' && !a.is_banned && !a.is_frozen);
    renderCampaignAccountsSelector();
}

async function loadBases() {
    try {
        const response = await fetch('/api/bases');
        const data = await response.json();
        bases = Array.isArray(data) ? data : [];
        renderBaseSelector();
    } catch (e) {
        console.error('Failed to load bases', e);
        bases = [];
        renderBaseSelector();
    }
}

function renderBaseSelector(selectedBaseId = null) {
    const select = document.getElementById('campaignBaseId');
    if (!select) return;
    const selected = Number(selectedBaseId || select.value || 0);
    const options = ['<option value="">Выберите базу</option>'];
    for (const b of bases) {
        const hint = `(${b.contacts_count || 0})`;
        const sel = Number(b.id) === selected ? 'selected' : '';
        options.push(`<option value="${escapeHtml(String(b.id))}" ${sel}>${escapeHtml(b.name)} ${hint}</option>`);
    }
    select.innerHTML = options.join('');
}

function campaignsUsingAccount(phone, excludeCampaignId = null) {
    return (campaigns || []).filter(c => {
        if (excludeCampaignId && c.id === excludeCampaignId) return false;
        const acc = Array.isArray(c.accounts) ? c.accounts : [];
        return acc.includes(phone);
    });
}

function updateCampaignAccountsSummary() {
    const el = document.getElementById('campaignAccountsSummary');
    if (!el) return;
    const selected = Array.from(selectedCampaignAccounts);
    if (!selected.length) {
        el.textContent = 'Не выбрано ни одного аккаунта.';
        return;
    }
    const busyElsewhere = selected.filter(phone => campaignsUsingAccount(phone, currentCampaignId).length > 0).length;
    el.textContent = `Выбрано аккаунтов: ${selected.length}. Используются в других кампаниях: ${busyElsewhere}.`;
}

function renderCampaignAccountsSelector() {
    const container = document.getElementById('campaignAccountsList');
    if (!container) return;

    if (!accounts.length) {
        container.innerHTML = '<div class="muted-text">Нет активных outreach-аккаунтов</div>';
        updateCampaignAccountsSummary();
        return;
    }

    container.innerHTML = accounts.map(a => {
        const phone = String(a.phone || '');
        const checked = selectedCampaignAccounts.has(phone) ? 'checked' : '';
        const usedIn = campaignsUsingAccount(phone, currentCampaignId);
        const usageHint = usedIn.length
            ? `Уже в: ${usedIn.map(c => escapeHtml(c.name)).join(', ')}`
            : 'Не используется в других кампаниях';
        const usedBadge = usedIn.length
            ? `<span class="badge badge-warning">в ${usedIn.length} камп.</span>`
            : `<span class="badge">свободен</span>`;
        return `
            <label class="account-checkitem">
                <input type="checkbox" class="campaign-account-checkbox" value="${escapeHtml(phone)}" ${checked}>
                <div class="account-checkitem-body">
                    <div class="account-checkitem-top">
                        <strong>${escapeHtml(phone)}</strong>
                        ${usedBadge}
                    </div>
                    <small class="muted-text">today: ${a.checked_today || 0} · limit: ${a.outreach_daily_limit || 20} · ${usageHint}</small>
                </div>
            </label>
        `;
    }).join('');

    container.querySelectorAll('.campaign-account-checkbox').forEach(input => {
        input.addEventListener('change', () => {
            const phone = input.value;
            if (input.checked) {
                selectedCampaignAccounts.add(phone);
            } else {
                selectedCampaignAccounts.delete(phone);
            }
            updateCampaignAccountsSummary();
        });
    });

    updateCampaignAccountsSummary();
}

async function loadTemplates() {
    const response = await fetch('/api/outreach/templates');
    templates = await response.json();

    const container = document.getElementById('templatesList');
    if (!container) return;
    if (!Array.isArray(templates) || templates.length === 0) {
        container.innerHTML = '<small class="muted-text">Шаблонов пока нет</small>';
        return;
    }
    container.innerHTML = templates.map(t =>
        `<button class="template-tag" data-id="${t.id}">${t.name}</button>`
    ).join('');

    container.querySelectorAll('.template-tag').forEach(btn => {
        btn.addEventListener('click', () => {
            const t = templates.find(x => String(x.id) === btn.dataset.id);
            if (t) document.getElementById('messageTemplate').value = t.text;
        });
    });
}

async function loadCampaigns() {
    const response = await fetch('/api/outreach/campaigns');
    campaigns = await response.json();

    const list = document.getElementById('campaignsList');
    if (!list) return;
    if (!Array.isArray(campaigns) || campaigns.length === 0) {
        list.innerHTML = '<div class="empty-state">Кампаний нет</div>';
        return;
    }

    list.innerHTML = campaigns.map(c => {
        const selected = c.id === currentCampaignId ? 'selected' : '';
        const progress = c.progress || 0;
        return `
            <button class="campaign-item ${selected}" data-id="${c.id}">
                <div class="campaign-item-head">
                    <span class="campaign-item-name">${escapeHtml(c.name)}</span>
                    <span class="badge ${statusBadgeClass(c.status)}">${escapeHtml(normalizeCampaignStatus(c.status))}</span>
                </div>
                <div class="campaign-item-meta">${c.sent_count || 0}/${c.total_contacts || 0} отправлено · ${c.reply_count || 0} ответов</div>
                <div class="progress"><div class="progress-bar" style="width:${progress}%"></div></div>
            </button>
        `;
    }).join('');

    list.querySelectorAll('.campaign-item').forEach(btn => {
        btn.addEventListener('click', () => {
            selectCampaign(Number(btn.dataset.id));
        });
    });

    renderCampaignAccountsSelector();
}

function restoreSelectedCampaign() {
    const savedId = Number(localStorage.getItem(SELECTED_CAMPAIGN_KEY));
    const toSelect = campaigns.find(c => c.id === savedId) || campaigns[0];
    if (toSelect) {
        selectCampaign(toSelect.id);
    } else {
        newCampaign();
    }
}

async function selectCampaign(campaignId) {
    currentCampaignId = campaignId;
    contactsPage = 1;
    localStorage.setItem(SELECTED_CAMPAIGN_KEY, String(campaignId));

    await loadCampaigns();

    const campaign = campaigns.find(c => c.id === campaignId);
    if (!campaign) return;

    populateCampaignForm(campaign);
    updateCampaignHeader(campaign);
    updateActionButtons(campaign.status);

    await Promise.all([
        loadContacts(campaignId),
        refreshReadiness(campaignId)
    ]);
}

function newCampaign() {
    currentCampaignId = null;
    contactsPage = 1;
    localStorage.removeItem(SELECTED_CAMPAIGN_KEY);

    document.getElementById('campaignName').value = '';
    document.getElementById('messageTemplate').value = '';
    renderBaseSelector(null);
    document.getElementById('dailyLimit').value = '20';
    document.getElementById('delayMin').value = '5';
    document.getElementById('delayMax').value = '15';
    document.getElementById('scheduleStartTime').value = '10:00';
    document.getElementById('scheduleEndTime').value = '20:00';
    document.getElementById('hourlyLimit').value = '10';
    document.getElementById('ignoreAfterHours').value = '24';

    const days = document.getElementById('scheduleDays');
    Array.from(days.options).forEach(o => {
        o.selected = ['1', '2', '3', '4', '5'].includes(o.value);
    });

    selectedCampaignAccounts = new Set();
    renderCampaignAccountsSelector();

    renderFollowupSteps([]);

    document.getElementById('campaignSelectionInfo').textContent = 'Новая кампания (ещё не сохранена)';
    document.getElementById('campaignStatusBadge').textContent = 'draft';
    document.getElementById('campaignStatusBadge').className = 'badge';
    document.getElementById('campaignProgressText').textContent = 'Прогресс: 0/0';

    document.getElementById('startCampaignBtn').disabled = true;
    document.getElementById('pauseCampaignBtn').disabled = true;
    document.getElementById('stopCampaignBtn').disabled = true;
    document.getElementById('deleteCampaignBtn').disabled = true;

    document.getElementById('contactsList').innerHTML = '<tr><td colspan="6" style="text-align:center; padding: 30px;">Сначала сохраните кампанию</td></tr>';
    const head = document.getElementById('contactsHead');
    if (head) head.innerHTML = '';
    const pagination = document.getElementById('contactsPagination');
    if (pagination) pagination.innerHTML = '';
    document.getElementById('contactsStats').textContent = '';
    document.getElementById('readinessStatus').textContent = '';
    document.getElementById('runStatus').textContent = '';
    document.getElementById('spintaxPreview').textContent = '';
}

function populateCampaignForm(campaign) {
    document.getElementById('campaignName').value = campaign.name || '';
    document.getElementById('messageTemplate').value = campaign.message_template || '';
    renderBaseSelector(campaign.base_id || null);
    document.getElementById('dailyLimit').value = campaign.daily_limit || 20;
    document.getElementById('delayMin').value = campaign.delay_min ?? 5;
    document.getElementById('delayMax').value = campaign.delay_max ?? 15;

    const selectedAccounts = campaign.accounts || [];
    selectedCampaignAccounts = new Set(selectedAccounts.map(String));
    renderCampaignAccountsSelector();

    const schedule = campaign.schedule || {};
    document.getElementById('scheduleStartTime').value = schedule.start_time || '10:00';
    document.getElementById('scheduleEndTime').value = schedule.end_time || '20:00';
    document.getElementById('hourlyLimit').value = schedule.hourly_limit || 10;
    document.getElementById('ignoreAfterHours').value = schedule.ignore_after_hours || 24;

    const selectedDays = schedule.days || [1, 2, 3, 4, 5];
    const days = document.getElementById('scheduleDays');
    Array.from(days.options).forEach(o => {
        o.selected = selectedDays.includes(parseInt(o.value, 10));
    });

    const strategy = campaign.strategy || { steps: [] };
    renderFollowupSteps(strategy.steps || []);
}

function updateCampaignHeader(campaign) {
    document.getElementById('campaignSelectionInfo').textContent = `Выбрана кампания: ${campaign.name}`;
    const badge = document.getElementById('campaignStatusBadge');
    badge.textContent = normalizeCampaignStatus(campaign.status);
    badge.className = `badge ${statusBadgeClass(campaign.status)}`;
    document.getElementById('campaignProgressText').textContent = `Прогресс: ${campaign.sent_count || 0}/${campaign.total_contacts || 0} · ответов ${campaign.reply_count || 0}`;
}

function updateActionButtons(status) {
    const st = normalizeCampaignStatus(status);
    const hasCampaign = Boolean(currentCampaignId);
    document.getElementById('startCampaignBtn').disabled = !hasCampaign || st === 'active';
    document.getElementById('pauseCampaignBtn').disabled = !hasCampaign || st !== 'active';
    document.getElementById('stopCampaignBtn').disabled = !hasCampaign || st === 'stopped' || st === 'draft';
    document.getElementById('deleteCampaignBtn').disabled = !hasCampaign;
}

function selectAllCampaignAccounts() {
    accounts.forEach(a => {
        if (a?.phone) selectedCampaignAccounts.add(String(a.phone));
    });
    renderCampaignAccountsSelector();
}

function clearCampaignAccounts() {
    selectedCampaignAccounts.clear();
    renderCampaignAccountsSelector();
}

function addFollowupStep(step = null) {
    const container = document.getElementById('followupSteps');
    const hint = container.querySelector('.muted-text');
    if (hint) hint.remove();
    const row = document.createElement('div');
    row.className = 'followup-step';

    const content = step?.content ?? '';

    row.innerHTML = `
        <div class="followup-step-grid">
            <div>
                <label class="followup-step-title">Фоллоуап</label>
                <textarea class="followup-content" rows="3" placeholder="Привет, {name}! {Напоминаю|Возвращаюсь} по теме...">${escapeHtml(content)}</textarea>
                <div class="variables-hint" style="margin-top:6px;">Переменные: {name} {company} {position} {phone}. Спинтакс: {вариант1|вариант2}</div>
            </div>
            <div class="followup-actions">
                <button type="button" class="btn btn-small followup-up">↑</button>
                <button type="button" class="btn btn-small followup-down">↓</button>
                <button type="button" class="btn btn-small followup-remove">Удалить</button>
            </div>
        </div>
    `;

    row.querySelector('.followup-up').addEventListener('click', () => {
        const prev = row.previousElementSibling;
        if (prev) {
            container.insertBefore(row, prev);
            renumberFollowupSteps();
        }
    });
    row.querySelector('.followup-down').addEventListener('click', () => {
        const next = row.nextElementSibling;
        if (next) {
            container.insertBefore(next, row);
            renumberFollowupSteps();
        }
    });
    row.querySelector('.followup-remove').addEventListener('click', () => row.remove());
    container.appendChild(row);
    renumberFollowupSteps();
}

function renderFollowupSteps(steps) {
    const container = document.getElementById('followupSteps');
    container.innerHTML = '';

    const normalized = (steps || []).map(step => ({
        content: step.content || ''
    }));

    if (normalized.length === 0) {
        const hint = document.createElement('div');
        hint.className = 'muted-text';
        hint.textContent = 'Шагов нет. Нажмите "+ Шаг", чтобы добавить фоллоуап.';
        container.appendChild(hint);
        return;
    }

    normalized.forEach(step => addFollowupStep(step));
    renumberFollowupSteps();
}

function renumberFollowupSteps() {
    const rows = Array.from(document.querySelectorAll('.followup-step'));
    rows.forEach((row, idx) => {
        const title = row.querySelector('.followup-step-title');
        if (title) {
            title.textContent = `Фоллоуап #${idx + 1}`;
        }
    });
}

function collectStrategy() {
    const rows = Array.from(document.querySelectorAll('.followup-step'));
    const steps = rows.map((row, idx) => {
        const content = (row.querySelector('.followup-content')?.value || '').trim();
        return {
            id: idx + 2,
            condition: 'ignored',
            content
        };
    }).filter(step => step.content.length > 0);

    return { steps };
}

function collectSchedule() {
    const days = Array.from(document.getElementById('scheduleDays').selectedOptions).map(o => parseInt(o.value, 10));
    return {
        start_time: document.getElementById('scheduleStartTime').value,
        end_time: document.getElementById('scheduleEndTime').value,
        hourly_limit: parseInt(document.getElementById('hourlyLimit').value, 10) || 10,
        ignore_after_hours: Math.max(1, parseInt(document.getElementById('ignoreAfterHours').value, 10) || 24),
        days
    };
}

function collectCampaignPayload() {
    const name = document.getElementById('campaignName').value.trim();
    const template = document.getElementById('messageTemplate').value.trim();
    const selectedAccounts = Array.from(selectedCampaignAccounts);

    const baseIdRaw = document.getElementById('campaignBaseId')?.value || '';
    const baseId = baseIdRaw ? Number(baseIdRaw) : null;

    return {
        name,
        template,
        base_id: Number.isFinite(baseId) && baseId > 0 ? baseId : null,
        accounts: selectedAccounts,
        dailyLimit: parseInt(document.getElementById('dailyLimit').value, 10) || 20,
        delayMin: parseInt(document.getElementById('delayMin').value, 10) || 0,
        delayMax: parseInt(document.getElementById('delayMax').value, 10) || 0,
        schedule: collectSchedule(),
        strategy: collectStrategy()
    };
}

async function saveCampaign() {
    const payload = collectCampaignPayload();
    if (!payload.name) {
        showSaveStatus('Введите название кампании', true);
        return;
    }
    if (!payload.template) {
        showSaveStatus('Введите текст первого сообщения', true);
        return;
    }
    if (!payload.base_id) {
        showSaveStatus('Выберите базу контактов', true);
        return;
    }
    if (!currentCampaignId && !payload.accounts.length) {
        showSaveStatus('Для новой кампании выберите минимум один outreach-аккаунт', true);
        return;
    }
    if (payload.delayMax < payload.delayMin) {
        showSaveStatus('delayMax не может быть меньше delayMin', true);
        return;
    }

    try {
        if (currentCampaignId) {
            const response = await fetch(`/api/outreach/campaign/${currentCampaignId}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok || data.error) {
                throw new Error(data.error || 'Ошибка сохранения');
            }
        } else {
            const response = await fetch('/api/outreach/campaigns', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            const data = await response.json();
            if (!response.ok || data.error) {
                throw new Error(data.error || 'Ошибка создания');
            }
            currentCampaignId = data.id;
            localStorage.setItem(SELECTED_CAMPAIGN_KEY, String(currentCampaignId));
        }

        showSaveStatus('Сохранено');
        await loadCampaigns();
        if (currentCampaignId) await selectCampaign(currentCampaignId);
    } catch (e) {
        showSaveStatus(e.message || 'Ошибка сохранения', true);
    }
}

async function saveTemplate() {
    const name = document.getElementById('templateName').value.trim();
    const text = document.getElementById('messageTemplate').value.trim();
    if (!name || !text) return;

    await fetch('/api/outreach/templates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, text })
    });

    document.getElementById('templateName').value = '';
    await loadTemplates();
    showSaveStatus('Шаблон сохранён');
}

async function previewSpintax() {
    const text = (document.getElementById('messageTemplate')?.value || '').trim();
    const previewEl = document.getElementById('spintaxPreview');
    if (!previewEl) return;
    if (!text) {
        previewEl.textContent = 'Введите текст сообщения, чтобы посмотреть варианты.';
        return;
    }

    previewEl.textContent = 'Генерирую варианты...';
    try {
        const response = await fetch('/api/outreach/spintax/preview', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text, samples: 5 })
        });
        const data = await response.json();
        if (!response.ok || data.error) {
            previewEl.textContent = data.error || 'Ошибка генерации';
            return;
        }
        const variants = Array.isArray(data.variants) ? data.variants : [];
        previewEl.innerHTML = variants.length
            ? variants.map((v, idx) => `${idx + 1}. ${escapeHtml(v)}`).join('<br>')
            : 'Варианты не сгенерированы';
    } catch (e) {
        previewEl.textContent = `Ошибка: ${e.message || e}`;
    }
}

function contactStatusClass(status) {
    const st = String(status || '').toLowerCase();
    if (st === 'replied') return 'badge-success';
    if (st === 'sent') return 'badge';
    if (st === 'ignored') return 'badge-warning';
    if (st === 'failed') return 'badge-danger';
    return 'badge';
}

async function loadContacts(campaignId) {
    const offset = Math.max(0, (contactsPage - 1) * CONTACTS_PAGE_SIZE);
    const response = await fetch(`/api/outreach/campaign/${campaignId}/contacts?limit=${CONTACTS_PAGE_SIZE}&offset=${offset}`);
    const data = await response.json();
    const contacts = Array.isArray(data) ? data : (Array.isArray(data.items) ? data.items : []);
    const total = Array.isArray(data) ? contacts.length : Number(data.total || 0);
    const statusCounts = Array.isArray(data)
        ? {
            new: contacts.filter(c => c.status === 'new').length,
            sent: contacts.filter(c => c.status === 'sent').length,
            ignored: contacts.filter(c => c.status === 'ignored').length,
            replied: contacts.filter(c => c.status === 'replied').length,
            failed: contacts.filter(c => c.status === 'failed').length
        }
        : (data.status_counts || { new: 0, sent: 0, ignored: 0, replied: 0, failed: 0 });
    const campaign = campaigns.find(c => c.id === campaignId);

    const tbody = document.getElementById('contactsList');
    if (total > 0 && contacts.length === 0 && contactsPage > 1) {
        contactsPage = Math.max(1, Math.ceil(total / CONTACTS_PAGE_SIZE));
        return loadContacts(campaignId);
    }
    if (!Array.isArray(contacts) || contacts.length === 0) {
        const head = document.getElementById('contactsHead');
        if (head) head.innerHTML = '';
        tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; padding: 30px;">Контактов пока нет</td></tr>';
        document.getElementById('contactsStats').textContent =
            `Всего ${total} · new ${Number(statusCounts.new || 0)} · sent ${Number(statusCounts.sent || 0)} · ignored ${Number(statusCounts.ignored || 0)} · replied ${Number(statusCounts.replied || 0)} · failed ${Number(statusCounts.failed || 0)}`;
        renderContactsPagination(total);
        return;
    }

    const hasCompany = contacts.some(c => (c.company || '').trim().length > 0);
    const hasPosition = contacts.some(c => (c.position || '').trim().length > 0);
    const hasAccountUsed = contacts.some(c => (c.account_used || '').trim().length > 0);
    const head = document.getElementById('contactsHead');
    if (head) {
        const cols = ['<th>Статус</th>', '<th>Контакт</th>', '<th>Шаг</th>'];
        if (hasCompany) cols.push('<th>Компания</th>');
        if (hasPosition) cols.push('<th>Должность</th>');
        if (hasAccountUsed) cols.push('<th>Аккаунт</th>');
        head.innerHTML = `<tr>${cols.join('')}</tr>`;
    }

    tbody.innerHTML = contacts.map(c => `
        <tr>
            <td><span class="badge ${contactStatusClass(c.status)}">${escapeHtml(c.status)}</span></td>
            <td>
                <strong>${escapeHtml(c.name || '—')}</strong><br>
                <small>${escapeHtml(c.phone || '—')} ${c.username ? '@' + escapeHtml(c.username) : ''} ${c.user_id ? `(ID: ${c.user_id})` : ''}</small>
            </td>
            <td><small>${renderContactStepInfo(c, campaign)}</small></td>
            ${hasCompany ? `<td>${escapeHtml(c.company || '-')}</td>` : ''}
            ${hasPosition ? `<td>${escapeHtml(c.position || '-')}</td>` : ''}
            ${hasAccountUsed ? `<td>${escapeHtml(c.account_used || '—')}</td>` : ''}
        </tr>
    `).join('');
    renderContactsPagination(total);

    document.getElementById('contactsStats').textContent =
        `Всего ${total} · new ${Number(statusCounts.new || 0)} · sent ${Number(statusCounts.sent || 0)} · ignored ${Number(statusCounts.ignored || 0)} · replied ${Number(statusCounts.replied || 0)} · failed ${Number(statusCounts.failed || 0)}`;
}

function renderContactsPagination(total) {
    const wrap = document.getElementById('contactsPagination');
    if (!wrap) return;
    const pages = Math.max(1, Math.ceil((Number(total) || 0) / CONTACTS_PAGE_SIZE));
    if (contactsPage > pages) contactsPage = pages;
    const start = total > 0 ? ((contactsPage - 1) * CONTACTS_PAGE_SIZE) + 1 : 0;
    const end = Math.min(contactsPage * CONTACTS_PAGE_SIZE, Number(total) || 0);

    wrap.innerHTML = `
        <button type="button" class="btn btn-small" id="contactsPrevPage" ${contactsPage <= 1 ? 'disabled' : ''}>← Назад</button>
        <small class="muted-text">Стр. ${contactsPage}/${pages} · показано ${start}-${end}</small>
        <button type="button" class="btn btn-small" id="contactsNextPage" ${contactsPage >= pages ? 'disabled' : ''}>Вперёд →</button>
    `;

    const prevBtn = document.getElementById('contactsPrevPage');
    const nextBtn = document.getElementById('contactsNextPage');
    if (prevBtn) {
        prevBtn.addEventListener('click', async () => {
            if (contactsPage <= 1 || !currentCampaignId) return;
            contactsPage -= 1;
            await loadContacts(currentCampaignId);
        });
    }
    if (nextBtn) {
        nextBtn.addEventListener('click', async () => {
            if (contactsPage >= pages || !currentCampaignId) return;
            contactsPage += 1;
            await loadContacts(currentCampaignId);
        });
    }
}

function getOrderedFollowupStepIds(campaign) {
    const steps = (campaign?.strategy?.steps || [])
        .map(s => Number(s?.id))
        .filter(id => Number.isFinite(id) && id >= 2)
        .sort((a, b) => a - b);
    return steps;
}

function renderContactStepInfo(contact, campaign) {
    const lastStep = Number(contact?.last_step_id || 0) || 0;
    const followupSteps = getOrderedFollowupStepIds(campaign);
    const nextStep = followupSteps.find(id => id > lastStep) || null;

    if (contact.status === 'new') {
        return 'следующий: первичное (шаг 1)';
    }
    if (!lastStep) {
        return 'шаг: —';
    }
    if (nextStep) {
        return `текущий: ${lastStep} · следующий: ${nextStep}`;
    }
    return `текущий: ${lastStep} · цепочка завершена`;
}

async function refreshReadiness(campaignId) {
    const response = await fetch(`/api/outreach/campaign/${campaignId}/readiness`);
    const info = await response.json();
    if (info.error) return;

    const problems = [];
    if (!info.has_api_credentials) problems.push('не заданы API ID/API Hash');
    if (!info.has_accounts) problems.push('нет активных outreach-аккаунтов в кампании');
    if ((info.contacts_total || 0) === 0) problems.push('не загружены контакты');
    if ((info.new_contacts_ready || 0) === 0) problems.push('нет новых контактов для отправки');
    if (!info.schedule_open_now) problems.push('сейчас вне окна расписания');

    const readinessText = problems.length
        ? `Сейчас не готово: ${problems.join(', ')}.`
        : 'Кампания готова к отправке.';

    document.getElementById('readinessStatus').textContent = readinessText;
    document.getElementById('runStatus').textContent =
        `Планировщик: ${info.scheduler_running ? 'запущен' : 'остановлен'} · аккаунтов в кампании ${info.accounts_valid}/${info.accounts_selected} · в blacklist ${info.contacts_blacklisted || 0}`;
}

function exportStepReport(format) {
    if (!currentCampaignId) {
        showSaveStatus('Сначала выберите кампанию', true);
        return;
    }
    const fmt = (format || 'csv').toLowerCase();
    window.open(`/api/outreach/campaign/${currentCampaignId}/step-report/export?format=${encodeURIComponent(fmt)}`, '_blank');
}

async function startCampaign(id) {
    const response = await fetch(`/api/outreach/campaign/${id}/start`, { method: 'POST' });
    const data = await response.json();
    if (!response.ok || data.error) {
        showSaveStatus(data.error || 'Ошибка запуска', true);
        return;
    }
    showSaveStatus('Кампания запущена');
    await Promise.all([loadCampaigns(), refreshReadiness(id), loadContacts(id)]);
    await selectCampaign(id);
}

async function pauseCampaign(id) {
    await fetch(`/api/outreach/campaign/${id}/pause`, { method: 'POST' });
    showSaveStatus('Кампания на паузе');
    await Promise.all([loadCampaigns(), refreshReadiness(id)]);
    await selectCampaign(id);
}

async function stopCampaign(id) {
    if (!confirm('Остановить кампанию?')) return;
    await fetch(`/api/outreach/campaign/${id}/stop`, { method: 'POST' });
    showSaveStatus('Кампания остановлена');
    await Promise.all([loadCampaigns(), refreshReadiness(id)]);
    await selectCampaign(id);
}

async function deleteCampaign(id) {
    if (!confirm('Удалить кампанию и все ее контакты/историю отправок?')) return;
    const response = await fetch(`/api/outreach/campaign/${id}`, { method: 'DELETE' });
    const data = await response.json();
    if (!response.ok || data.error) {
        showSaveStatus(data.error || 'Ошибка удаления кампании', true);
        return;
    }
    showSaveStatus('Кампания удалена');
    currentCampaignId = null;
    localStorage.removeItem(SELECTED_CAMPAIGN_KEY);
    await loadCampaigns();
    restoreSelectedCampaign();
}

function escapeHtml(value) {
    return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
}
