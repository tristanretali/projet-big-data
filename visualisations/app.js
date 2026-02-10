const API_BASE = 'http://localhost:8000/api/v1';
let token = null;
let charts = {};

// ============= AUTHENTICATION =============

async function login() {
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;
    const statusDiv = document.getElementById('loginStatus');

    if (!username || !password) {
        statusDiv.innerHTML = '<div class="error">Veuillez remplir tous les champs</div>';
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });

        if (response.ok) {
            const data = await response.json();
            token = data.access_token;
            statusDiv.innerHTML = '<div class="success">✓ Connexion réussie!</div>';
            
            setTimeout(() => {
                document.getElementById('loginSection').style.display = 'none';
                document.getElementById('dashboardSection').style.display = 'block';
                document.getElementById('userName').textContent = username;
                loadAllData(); // Charge TOUT au démarrage
            }, 500);
        } else {
            statusDiv.innerHTML = '<div class="error">✗ Identifiants incorrects</div>';
        }
    } catch (error) {
        statusDiv.innerHTML = `<div class="error">✗ Erreur: ${error.message}</div>`;
    }
}

function logout() {
    token = null;
    document.getElementById('loginSection').style.display = 'block';
    document.getElementById('dashboardSection').style.display = 'none';
    Object.values(charts).forEach(chart => chart?.destroy());
    charts = {};
}

// ============= NAVIGATION =============

function scrollToSection(sectionId) {
    const element = document.getElementById(`${sectionId}-tab`);
    if (element) {
        element.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
    
    // Update active button
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    event.target.classList.add('active');
}

// ============= API HELPER =============

async function fetchAPI(endpoint) {
    const response = await fetch(`${API_BASE}${endpoint}`, {
        headers: { 'Authorization': `Bearer ${token}` }
    });
    
    if (response.status === 401) {
        alert('Session expirée. Reconnexion requise.');
        logout();
        throw new Error('Unauthorized');
    }
    
    if (!response.ok) {
        throw new Error(`API Error: ${response.status}`);
    }
    
    return response.json();
}

// ============= LOAD ALL DATA =============

async function loadAllData() {
    await loadOverview();
    await loadCountries();
    await loadProducts();
    await loadReturns();
    await loadPeriod();
}

// ============= OVERVIEW =============

async function loadOverview() {
    try {
        const [countries, products, returns, periods] = await Promise.all([
            fetchAPI('/sales_per_country?page=1&page_size=1'),
            fetchAPI('/top_products?page=1&page_size=1'),
            fetchAPI('/return_products?page=1&page_size=1'),
            fetchAPI('/periodic?page=1&page_size=1')
        ]);
        
        document.getElementById('totalCountries').textContent = countries.total;
        document.getElementById('totalProducts').textContent = products.total;
        document.getElementById('totalReturns').textContent = returns.total;
        document.getElementById('totalDays').textContent = periods.total;
    } catch (error) {
        console.error('Erreur chargement vue d\'ensemble:', error);
    }
}

// ============= COUNTRIES =============

async function loadCountries() {
    await loadCountriesChart();
    await loadCountriesTable(1);
}

async function loadCountriesChart() {
    try {
        const data = await fetchAPI('/sales_per_country?page=1&page_size=15');
        
        const countries = data.data.map(item => item.Country);
        const sales = data.data.map(item => parseFloat(item.total_sales));

        if (charts.countries) charts.countries.destroy();
        
        const ctx = document.getElementById('countriesChart').getContext('2d');
        charts.countries = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: countries,
                datasets: [{
                    label: 'Chiffre d\'affaires (€)',
                    data: sales,
                    backgroundColor: 'rgba(102, 126, 234, 0.7)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: true }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: value => value.toLocaleString('fr-FR') + ' €'
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Erreur chargement graphique pays:', error);
    }
}

async function loadCountriesTable(page) {
    try {
        const data = await fetchAPI(`/sales_per_country?page=${page}&page_size=20`);
        const tbody = document.getElementById('countriesTableBody');
        
        tbody.innerHTML = data.data.map(item => `
            <tr>
                <td>${item.Country}</td>
                <td>${parseFloat(item.total_sales).toLocaleString('fr-FR', {minimumFractionDigits: 2})} €</td>
                <td>${parseInt(item.number_of_orders).toLocaleString('fr-FR')}</td>
                <td>${parseInt(item.total_quantity).toLocaleString('fr-FR')}</td>
            </tr>
        `).join('');
        
        updatePagination('countries', data, page);
    } catch (error) {
        console.error('Erreur chargement table pays:', error);
    }
}

// ============= PRODUCTS =============

async function loadProducts() {
    await loadProductsChart();
    await loadProductsTable(1);
}

async function loadProductsChart() {
    try {
        const data = await fetchAPI('/top_products?page=1&page_size=15');
        
        const products = data.data.map(item => 
            item.Description.length > 25 ? item.Description.substring(0, 25) + '...' : item.Description
        );
        const revenues = data.data.map(item => parseFloat(item.total_revenue));

        if (charts.products) charts.products.destroy();
        
        const ctx = document.getElementById('productsChart').getContext('2d');
        charts.products = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: products,
                datasets: [{
                    label: 'Revenus (€)',
                    data: revenues,
                    backgroundColor: 'rgba(46, 204, 113, 0.7)',
                    borderColor: 'rgba(46, 204, 113, 1)',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                indexAxis: 'y',
                plugins: {
                    legend: { display: true }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            callback: value => value.toLocaleString('fr-FR') + ' €'
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Erreur chargement graphique produits:', error);
    }
}

async function loadProductsTable(page) {
    try {
        const data = await fetchAPI(`/top_products?page=${page}&page_size=20`);
        const tbody = document.getElementById('productsTableBody');
        
        tbody.innerHTML = data.data.map(item => `
            <tr>
                <td>${item.StockCode}</td>
                <td>${item.Description}</td>
                <td>${parseFloat(item.total_revenue).toLocaleString('fr-FR', {minimumFractionDigits: 2})} €</td>
                <td>${parseInt(item.total_quantity_sold).toLocaleString('fr-FR')}</td>
                <td>${parseInt(item.number_of_sales).toLocaleString('fr-FR')}</td>
            </tr>
        `).join('');
        
        updatePagination('products', data, page);
    } catch (error) {
        console.error('Erreur chargement table produits:', error);
    }
}

// ============= RETURNS =============

async function loadReturns() {
    await loadReturnsChart();
    await loadReturnsTable(1);
}

async function loadReturnsChart() {
    try {
        const data = await fetchAPI('/return_products?page=1&page_size=15');
        
        const products = data.data.map(item => 
            item.Description.length > 25 ? item.Description.substring(0, 25) + '...' : item.Description
        );
        const losses = data.data.map(item => Math.abs(parseFloat(item.total_loss)));

        if (charts.returns) charts.returns.destroy();
        
        const ctx = document.getElementById('returnsChart').getContext('2d');
        charts.returns = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: products,
                datasets: [{
                    label: 'Perte (€)',
                    data: losses,
                    backgroundColor: 'rgba(231, 76, 60, 0.7)',
                    borderColor: 'rgba(231, 76, 60, 1)',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                indexAxis: 'y',
                plugins: {
                    legend: { display: true }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            callback: value => value.toLocaleString('fr-FR') + ' €'
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Erreur chargement graphique retours:', error);
    }
}

async function loadReturnsTable(page) {
    try {
        const data = await fetchAPI(`/return_products?page=${page}&page_size=20`);
        const tbody = document.getElementById('returnsTableBody');
        
        tbody.innerHTML = data.data.map(item => `
            <tr>
                <td>${item.StockCode}</td>
                <td>${item.Description}</td>
                <td>${parseInt(item.total_returned).toLocaleString('fr-FR')}</td>
                <td>${parseInt(item.number_of_returns).toLocaleString('fr-FR')}</td>
                <td>${parseFloat(item.total_loss).toLocaleString('fr-FR', {minimumFractionDigits: 2})} €</td>
            </tr>
        `).join('');
        
        updatePagination('returns', data, page);
    } catch (error) {
        console.error('Erreur chargement table retours:', error);
    }
}

// ============= PERIOD =============

async function loadPeriod() {
    await loadPeriodChart();
    await loadPeriodTable(1);
}

async function loadPeriodChart() {
    try {
        const yearSelect = document.getElementById('filterYear');
        const monthSelect = document.getElementById('filterMonth');
        
        const year = yearSelect ? yearSelect.value : '';
        const month = monthSelect ? monthSelect.value : '';
        
        let endpoint = '/periodic?page=1&page_size=30';
        if (year) endpoint += `&year=${year}`;
        if (month) endpoint += `&month=${month}`;
        
        const data = await fetchAPI(endpoint);
        
        const labels = data.data.map(item => 
            `${item.year}-${String(item.month).padStart(2, '0')}-${String(item.day).padStart(2, '0')}`
        );
        const sales = data.data.map(item => parseFloat(item.total_sales));

        if (charts.period) charts.period.destroy();
        
        const ctx = document.getElementById('periodChart').getContext('2d');
        charts.period = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Chiffre d\'affaires (€)',
                    data: sales,
                    backgroundColor: 'rgba(155, 89, 182, 0.3)',
                    borderColor: 'rgba(155, 89, 182, 1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: true }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: value => value.toLocaleString('fr-FR') + ' €'
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Erreur chargement graphique périodes:', error);
    }
}

async function loadPeriodTable(page) {
    try {
        const yearSelect = document.getElementById('filterYear');
        const monthSelect = document.getElementById('filterMonth');
        
        const year = yearSelect ? yearSelect.value : '';
        const month = monthSelect ? monthSelect.value : '';
        
        let endpoint = `/periodic?page=${page}&page_size=20`;
        if (year) endpoint += `&year=${year}`;
        if (month) endpoint += `&month=${month}`;
        
        const data = await fetchAPI(endpoint);
        const tbody = document.getElementById('periodTableBody');
        
        tbody.innerHTML = data.data.map(item => `
            <tr>
                <td>${item.year}-${String(item.month).padStart(2, '0')}-${String(item.day).padStart(2, '0')}</td>
                <td>${parseFloat(item.total_sales).toLocaleString('fr-FR', {minimumFractionDigits: 2})} €</td>
                <td>${parseInt(item.number_of_orders).toLocaleString('fr-FR')}</td>
                <td>${parseInt(item.total_quantity).toLocaleString('fr-FR')}</td>
            </tr>
        `).join('');
        
        updatePagination('period', data, page);
    } catch (error) {
        console.error('Erreur chargement table périodes:', error);
    }
}

function applyPeriodFilters() {
    loadPeriod();
}

function clearPeriodFilters() {
    document.getElementById('filterYear').value = '';
    document.getElementById('filterMonth').value = '';
    loadPeriod();
}

// ============= PAGINATION =============

function updatePagination(type, data, currentPage) {
    const totalPages = data.total_pages || Math.ceil(data.total / 20);
    const paginationDiv = document.getElementById(`${type}Pagination`);
    
    paginationDiv.innerHTML = `
        <button ${currentPage === 1 ? 'disabled' : ''} onclick="load${capitalize(type)}Table(${currentPage - 1})">
            ← Précédent
        </button>
        <span>Page ${currentPage} / ${totalPages}</span>
        <button ${currentPage >= totalPages ? 'disabled' : ''} onclick="load${capitalize(type)}Table(${currentPage + 1})">
            Suivant →
        </button>
    `;
}

function capitalize(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

// ============= EVENT LISTENERS =============

document.addEventListener('DOMContentLoaded', () => {
    const passwordInput = document.getElementById('password');
    const usernameInput = document.getElementById('username');
    
    [passwordInput, usernameInput].forEach(input => {
        input?.addEventListener('keypress', e => {
            if (e.key === 'Enter') login();
        });
    });
});