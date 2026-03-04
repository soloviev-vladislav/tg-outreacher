const socket = io();

// Элементы DOM
const statusIndicator = document.getElementById('statusIndicator');
const statusText = document.getElementById('statusText');
const startBtn = document.getElementById('startBtn');
const stopBtn = document.getElementById('stopBtn');
const clearBtn = document.getElementById('clearBtn');
const processedCount = document.getElementById('processedCount');
const totalCount = document.getElementById('totalCount');
const registeredCount = document.getElementById('registeredCount');
const notRegisteredCount = document.getElementById('notRegisteredCount');
const errorCount = document.getElementById('errorCount');
const progressBar = document.getElementById('progressBar');
const currentPhone = document.getElementById('currentPhone');
const timeRemaining = document.getElementById('timeRemaining');
const accountsList = document.getElementById('accountsList');
const recentResults = document.getElementById('recentResults');
const filesList = document.getElementById('filesList');
const phonesTextarea = document.getElementById('phones');
const fileInput = document.getElementById('fileInput');
const fileName = document.getElementById('fileName');
const uploadArea = document.getElementById('uploadArea');

// График
let statsChart;
let chartData = {
    labels: ['Найдено', 'Не найдено', 'Ошибки'],
    datasets: [{
        data: [0, 0, 0],
        backgroundColor: ['#2ecc71', '#f39c12', '#e74c3c']
    }]
};

// Инициализация графика
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM загружен, инициализация...');
    
    // Проверяем, есть ли элемент для графика
    const chartElement = document.getElementById('statsChart');
    if (chartElement) {
        const ctx = chartElement.getContext('2d');
        statsChart = new Chart(ctx, {
            type: 'doughnut',
            data: chartData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
        console.log('График инициализирован');
    } else {
        console.warn('Элемент statsChart не найден');
    }

    // Загружаем список аккаунтов (только checker)
    loadCheckerAccounts();
    loadSavedConfig();
    loadLastHistory();

    // Загружаем список файлов
    loadFiles();
    
    // Добавляем обработчики для drag & drop
    setupDragAndDrop();
    
    // Добавляем обработчики кнопок
    setupButtonHandlers();
});

async function loadSavedConfig() {
    try {
        const response = await fetch('/api/config');
        const data = await response.json();
        if (!response.ok || !data) return;

        const targetChatInput = document.getElementById('targetChat');
        const delayInput = document.getElementById('delay');
        if (targetChatInput && data.target_chat) targetChatInput.value = data.target_chat;
        if (delayInput && data.delay) delayInput.value = data.delay;
        if (phonesTextarea && data.phones_text) phonesTextarea.value = data.phones_text;
    } catch (error) {
        console.error('Error loading saved config:', error);
    }
}

async function loadLastHistory() {
    try {
        const response = await fetch('/api/history?limit=20');
        const history = await response.json();
        if (!response.ok || !Array.isArray(history) || history.length === 0) return;

        if (recentResults) recentResults.innerHTML = '';
        let reg = 0;
        let nreg = 0;
        let err = 0;

        history
            .slice()
            .reverse()
            .forEach(item => {
                const status = item.registered ? 'registered' : 'not_registered';
                if (item.registered) reg += 1;
                else nreg += 1;
                addResult({
                    phone: item.phone,
                    status,
                    user_info: {
                        user_id: item.user_id,
                        username: item.username,
                        first_name: item.first_name,
                        last_name: item.last_name
                    },
                    message: status === 'registered' ? 'Найден' : 'Не зарегистрирован'
                });
            });

        if (registeredCount) registeredCount.textContent = reg;
        if (notRegisteredCount) notRegisteredCount.textContent = nreg;
        if (errorCount) errorCount.textContent = err;
        if (processedCount) processedCount.textContent = reg + nreg + err;
        if (totalCount) totalCount.textContent = `/${reg + nreg + err}`;
        if (progressBar) progressBar.style.width = '100%';
        if (timeRemaining) timeRemaining.textContent = 'Осталось: 0 сек';

        chartData.datasets[0].data = [reg, nreg, err];
        if (statsChart) statsChart.update();
    } catch (error) {
        console.error('Error loading last history:', error);
    }
}

// Настройка обработчиков кнопок
function setupButtonHandlers() {
    // Кнопка Старт
    if (startBtn) {
        startBtn.addEventListener('click', async () => {
            console.log('Start button clicked');
            
            const targetChat = document.getElementById('targetChat').value;
            const delay = document.getElementById('delay').value;
            const phones = phonesTextarea ? phonesTextarea.value : '';
            
            if (!targetChat || !phones) {
                alert('Пожалуйста, заполните целевой чат и номера телефонов');
                return;
            }
            
            // Безопасное обновление UI
            if (startBtn) startBtn.disabled = true;
            if (stopBtn) stopBtn.disabled = false;
            if (statusIndicator) statusIndicator.className = 'status-indicator active';
            if (statusText) statusText.textContent = 'Работает';
            
            try {
                console.log('Sending request to /api/start');
                const response = await fetch('/api/start', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        target_chat: targetChat,
                        phones: phones,
                        delay: delay
                    })
                });
                
                const data = await response.json();
                console.log('Response:', data);
                
                if (data.error) {
                    alert(data.error);
                    if (startBtn) startBtn.disabled = false;
                    if (stopBtn) stopBtn.disabled = true;
                    if (statusIndicator) statusIndicator.className = 'status-indicator stopped';
                    if (statusText) statusText.textContent = 'Ошибка';
                }
            } catch (error) {
                console.error('Error starting checker:', error);
                alert('Ошибка при запуске: ' + error.message);
                if (startBtn) startBtn.disabled = false;
                if (stopBtn) stopBtn.disabled = true;
                if (statusIndicator) statusIndicator.className = 'status-indicator stopped';
                if (statusText) statusText.textContent = 'Ошибка';
            }
        });
        console.log('Обработчик для Start добавлен');
    } else {
        console.warn('Кнопка Start не найдена');
    }
    
    // Кнопка Стоп
    if (stopBtn) {
        stopBtn.addEventListener('click', async () => {
            try {
                await fetch('/api/stop', { method: 'POST' });
                if (statusIndicator) statusIndicator.className = 'status-indicator stopped';
                if (statusText) statusText.textContent = 'Останавливается...';
            } catch (error) {
                console.error('Error stopping checker:', error);
            }
        });
    }
    
    // Кнопка Очистить
    if (clearBtn) {
        clearBtn.addEventListener('click', () => {
            if (phonesTextarea) phonesTextarea.value = '';
            if (recentResults) recentResults.innerHTML = '';
            if (processedCount) processedCount.textContent = '0';
            if (registeredCount) registeredCount.textContent = '0';
            if (notRegisteredCount) notRegisteredCount.textContent = '0';
            if (errorCount) errorCount.textContent = '0';
            if (currentPhone) currentPhone.textContent = '-';
            if (progressBar) progressBar.style.width = '0%';
            if (timeRemaining) timeRemaining.textContent = 'расчет...';
            
            chartData.datasets[0].data = [0, 0, 0];
            if (statsChart) statsChart.update();
        });
    }
}

// Загрузка аккаунтов только с типом checker
async function loadCheckerAccounts() {
    try {
        const response = await fetch('/api/accounts/detailed');
        const allAccounts = await response.json();
        // Фильтруем только checker аккаунты, которые не забанены
        const checkerAccounts = allAccounts.filter(a => 
            a.account_type === 'checker' && !a.is_banned && !a.is_frozen
        );
        updateAccountsList(checkerAccounts);
    } catch (error) {
        console.error('Error loading checker accounts:', error);
    }
}

// Загрузка файлов
async function loadFiles() {
    // Здесь можно добавить загрузку списка файлов с сервера
}

// Настройка drag & drop
function setupDragAndDrop() {
    if (!uploadArea) return;
    
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.style.background = '#e0e0e0';
    });

    uploadArea.addEventListener('dragleave', (e) => {
        e.preventDefault();
        uploadArea.style.background = '#f8f9fa';
    });

    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.style.background = '#f8f9fa';
        
        const file = e.dataTransfer.files[0];
        if (file && fileInput) {
            fileInput.files = e.dataTransfer.files;
            if (fileName) fileName.textContent = file.name;
            
            // Триггерим событие change
            const event = new Event('change', { bubbles: true });
            fileInput.dispatchEvent(event);
        }
    });
    
    // Клик по области загрузки
    uploadArea.addEventListener('click', () => {
        if (fileInput) fileInput.click();
    });
}

// Обновление списка аккаунтов
function updateAccountsList(accounts) {
    if (!accountsList) return;
    accountsList.innerHTML = '';
    
    if (accounts.length === 0) {
        accountsList.innerHTML = '<div class="list-item" style="text-align:center; color:#86868b;">Нет доступных checker аккаунтов</div>';
        return;
    }
    
    accounts.forEach(account => {
        const accountDiv = document.createElement('div');
        accountDiv.className = 'account-item';
        accountDiv.innerHTML = `
            <span class="account-phone">${account.phone}</span>
            <span class="account-stats">${account.checked_today}/20</span>
            <div class="account-status available"></div>
        `;
        accountsList.appendChild(accountDiv);
    });
}

// Socket.IO события
socket.on('connect', () => {
    console.log('Connected to server');
});

socket.on('progress_update', (data) => {
    updateProgress(data);
});

socket.on('accounts_update', (accounts) => {
    updateAccountStats(accounts);
});

socket.on('phone_result', (data) => {
    addResult(data);
});

socket.on('checker_complete', (data) => {
    if (statusIndicator) statusIndicator.className = 'status-indicator stopped';
    if (statusText) statusText.textContent = 'Завершено';
    if (startBtn) startBtn.disabled = false;
    if (stopBtn) stopBtn.disabled = true;
    
    alert(`Проверка завершена!\nНайдено: ${data.registered}\nНе найдено: ${data.not_registered}\nОшибки: ${data.errors}`);
});

socket.on('checker_stopped', () => {
    if (statusIndicator) statusIndicator.className = 'status-indicator stopped';
    if (statusText) statusText.textContent = 'Остановлено';
    if (startBtn) startBtn.disabled = false;
    if (stopBtn) stopBtn.disabled = true;
});

socket.on('file_ready', (data) => {
    addFileToList(data);
});

socket.on('error', (data) => {
    alert('Ошибка: ' + data.message);
});

// Обновление прогресса
function updateProgress(data) {
    if (processedCount) processedCount.textContent = data.processed;
    if (totalCount) totalCount.textContent = `/${data.total}`;
    if (registeredCount) registeredCount.textContent = data.registered;
    if (notRegisteredCount) notRegisteredCount.textContent = data.not_registered;
    if (errorCount) errorCount.textContent = data.errors;
    if (currentPhone) currentPhone.textContent = data.current_phone || '-';
    
    const percent = (data.processed / data.total) * 100;
    if (progressBar) progressBar.style.width = percent + '%';
    
    if (data.estimated_time && timeRemaining) {
        const minutes = Math.floor(data.estimated_time / 60);
        const seconds = Math.floor(data.estimated_time % 60);
        timeRemaining.textContent = `${minutes} мин ${seconds} сек`;
    }
    
    // Обновляем график
    chartData.datasets[0].data = [
        data.registered,
        data.not_registered,
        data.errors
    ];
    if (statsChart) statsChart.update();
}

// Обновление статистики аккаунтов (для сокетов)
function updateAccountStats(accounts) {
    if (!accountsList) return;
    accountsList.innerHTML = '';
    
    // Фильтруем только checker аккаунты
    const checkerAccounts = accounts.filter(
        a => !a.account_type || a.account_type === 'checker'
    );
    
    if (checkerAccounts.length === 0) {
        accountsList.innerHTML = '<div class="list-item" style="text-align:center; color:#86868b;">Нет доступных checker аккаунтов</div>';
        return;
    }
    
    checkerAccounts.forEach(account => {
        const accountDiv = document.createElement('div');
        accountDiv.className = 'account-item';
        accountDiv.innerHTML = `
            <span class="account-phone">${account.phone}</span>
            <span class="account-stats">${account.checked_today}/20 (${account.total_checked})</span>
            <div class="account-status ${account.can_use ? 'available' : 'unavailable'}"></div>
        `;
        accountsList.appendChild(accountDiv);
    });
}

// Добавление результата в список
function addResult(data) {
    if (!recentResults) return;
    
    const resultDiv = document.createElement('div');
    resultDiv.className = `result-item ${data.status}`;
    
    let message = `<strong>${data.phone}</strong><br>`;
    
    switch(data.status) {
        case 'registered':
            message += `✅ Найден: ${data.user_info.first_name || ''} ${data.user_info.last_name || ''}<br>`;
            message += `ID: ${data.user_info.user_id}<br>`;
            if (data.user_info.username) {
                message += `@${data.user_info.username}`;
            }
            break;
        case 'not_registered':
            message += `❌ Не зарегистрирован`;
            break;
        case 'invalid':
            message += `⚠️ ${data.message}`;
            break;
        case 'error':
            message += `🔴 ${data.message}`;
            break;
    }
    
    resultDiv.innerHTML = message;
    recentResults.insertBefore(resultDiv, recentResults.firstChild);
    
    // Ограничиваем количество результатов
    if (recentResults.children.length > 20) {
        recentResults.removeChild(recentResults.lastChild);
    }
}

// Добавление файла в список
function addFileToList(data) {
    if (!filesList) return;
    
    const fileDiv = document.createElement('div');
    fileDiv.className = 'file-item';
    
    const date = new Date().toLocaleString();
    
    fileDiv.innerHTML = `
        <a href="/api/download/${data.filename}" class="file-name" download>${data.filename}</a>
        <span class="file-size">${date}</span>
    `;
    
    filesList.insertBefore(fileDiv, filesList.firstChild);
}

// Загрузка файла
if (fileInput) {
    fileInput.addEventListener('change', async (e) => {
        const file = e.target.files[0];
        if (!file) return;
        
        if (fileName) fileName.textContent = file.name;
        
        const formData = new FormData();
        formData.append('file', file);
        
        try {
            const response = await fetch('/api/upload', {
                method: 'POST',
                body: formData
            });
            
            const data = await response.json();
            if (data.phones && phonesTextarea) {
                phonesTextarea.value = data.phones.join('\n');
            } else if (data.error) {
                alert('Ошибка загрузки: ' + data.error);
            }
        } catch (error) {
            console.error('Error uploading file:', error);
            alert('Ошибка при загрузке файла');
        }
    });
}
