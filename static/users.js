let users = [];
let overview = null;

document.addEventListener('DOMContentLoaded', () => {
    reloadUsers();
});

async function apiJson(url, options = {}) {
    const response = await fetch(url, options);
    let payload = {};
    try {
        payload = await response.json();
    } catch (e) {
        payload = {};
    }
    if (!response.ok) {
        throw new Error(payload.error || `HTTP ${response.status}`);
    }
    return payload;
}

async function reloadUsers() {
    try {
        overview = await apiJson('/api/admin/users/overview');
        users = await apiJson('/api/auth/users');
        renderStats();
        renderTable();
    } catch (e) {
        document.getElementById('usersTableBody').innerHTML =
            `<tr><td colspan="8" style="text-align:center; padding:40px; color:#c0392b;">${escapeHtml(e.message || 'Ошибка загрузки')}</td></tr>`;
    }
}

function renderStats() {
    const s = (overview && overview.summary) || {};
    document.getElementById('usersTotal').textContent = Number(s.users_total || 0);
    document.getElementById('usersActive').textContent = Number(s.users_active || 0);
    document.getElementById('usersPending').textContent = Number(s.users_pending || 0);
    document.getElementById('sendsToday').textContent = Number(s.sends_today || 0);
}

function renderTable() {
    const tbody = document.getElementById('usersTableBody');
    if (!Array.isArray(users) || users.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" style="text-align:center; padding:40px;">Пользователей нет</td></tr>';
        return;
    }

    tbody.innerHTML = users.map(u => {
        const statusBadge = u.is_active
            ? '<span class="badge badge-success">active</span>'
            : '<span class="badge badge-warning">pending</span>';
        const campaigns = `${num(u.campaigns_count)} (active ${num(u.active_campaigns_count)})`;
        const sends = `${num(u.sent_total)} (1st ${num(u.sent_initial)} / fu ${num(u.sent_followup)})`;
        return `
            <tr>
                <td>
                    <div><strong>${escapeHtml(u.username || '')}</strong></div>
                    <div class="muted-text">${escapeHtml(String(u.created_at || ''))}</div>
                </td>
                <td>
                    <select class="form-control" onchange="changeRole(${u.id}, this.value)">
                        <option value="user" ${u.role === 'user' ? 'selected' : ''}>user</option>
                        <option value="admin" ${u.role === 'admin' ? 'selected' : ''}>admin</option>
                    </select>
                </td>
                <td>${statusBadge}</td>
                <td>${num(u.sessions_count)}</td>
                <td>${campaigns}</td>
                <td>${sends}</td>
                <td>${num(u.sent_today)}</td>
                <td>
                    <div class="btn-group wrap">
                        ${u.is_active
                            ? `<button class="btn btn-small btn-warning" onclick="setActive(${u.id}, false)">Block</button>`
                            : `<button class="btn btn-small btn-success" onclick="setActive(${u.id}, true)">Approve</button>`
                        }
                        <button class="btn btn-small" onclick="resetPassword(${u.id})">Пароль</button>
                        <button class="btn btn-small btn-danger" onclick="deleteUser(${u.id})">Удалить</button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

async function setActive(userId, value) {
    try {
        await apiJson(`/api/auth/users/${encodeURIComponent(userId)}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ is_active: !!value })
        });
        await reloadUsers();
    } catch (e) {
        alert(e.message || 'Ошибка изменения статуса');
    }
}

async function changeRole(userId, role) {
    try {
        await apiJson(`/api/auth/users/${encodeURIComponent(userId)}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ role })
        });
        await reloadUsers();
    } catch (e) {
        alert(e.message || 'Ошибка изменения роли');
        await reloadUsers();
    }
}

async function resetPassword(userId) {
    const pwd = prompt('Новый пароль (минимум 6 символов):');
    if (pwd === null) return;
    if (!pwd || pwd.length < 6) {
        alert('Пароль слишком короткий');
        return;
    }
    try {
        await apiJson(`/api/auth/users/${encodeURIComponent(userId)}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ password: pwd })
        });
        alert('Пароль обновлен');
    } catch (e) {
        alert(e.message || 'Ошибка обновления пароля');
    }
}

async function deleteUser(userId) {
    if (!confirm('Удалить пользователя?')) return;
    try {
        await apiJson(`/api/auth/users/${encodeURIComponent(userId)}`, { method: 'DELETE' });
        await reloadUsers();
    } catch (e) {
        alert(e.message || 'Ошибка удаления пользователя');
    }
}

function openCreateUserModal() {
    document.getElementById('newUserName').value = '';
    document.getElementById('newUserPassword').value = '';
    document.getElementById('newUserRole').value = 'user';
    document.getElementById('createUserError').textContent = '';
    document.getElementById('createUserModal').classList.add('show');
}

function closeCreateUserModal() {
    document.getElementById('createUserModal').classList.remove('show');
}

async function createUser() {
    const username = (document.getElementById('newUserName').value || '').trim();
    const password = document.getElementById('newUserPassword').value || '';
    const role = document.getElementById('newUserRole').value || 'user';
    const errEl = document.getElementById('createUserError');
    errEl.textContent = '';

    if (!username) {
        errEl.textContent = 'Укажите логин';
        return;
    }
    if (password.length < 6) {
        errEl.textContent = 'Пароль минимум 6 символов';
        return;
    }

    try {
        await apiJson('/api/auth/users', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password, role })
        });
        closeCreateUserModal();
        await reloadUsers();
    } catch (e) {
        errEl.textContent = e.message || 'Ошибка создания пользователя';
    }
}

function escapeHtml(value) {
    return String(value || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function num(value) {
    return Number(value || 0);
}
