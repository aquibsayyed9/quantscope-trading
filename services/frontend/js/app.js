const API_BASE = 'http://localhost:8080';

let currentUser = 'alice';
let ws = null;
let currentSymbol = 'BTCUSDT';

const WS_URL = `ws://localhost:8080/ws/notifications?user_id=${currentUser}`;
// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    initializeApp();
});

function initializeApp() {
    setupEventListeners();
    connectWebSocket();
    loadOrderBook();
    loadRecentTrades();
    loadUserOrders();
    // Refresh data periodically
    setInterval(loadOrderBook, 2000);
    setInterval(loadRecentTrades, 3000);
    setInterval(loadUserOrders, 5000);
}

function setupEventListeners() {
    // User selection
    document.getElementById('userSelect').addEventListener('change', (e) => {
        currentUser = e.target.value;
        loadUserOrders();
    });

    // Order form submission
    document.getElementById('orderForm').addEventListener('submit', handleOrderSubmit);

    // Order type change - disable price for market orders
    document.getElementById('orderType').addEventListener('change', (e) => {
        const priceInput = document.getElementById('price');
        priceInput.disabled = e.target.value === 'MARKET';
    });
}

async function handleOrderSubmit(e) {
    e.preventDefault();
    
    const orderData = {
        user_id: currentUser,
        symbol: document.getElementById('symbol').value.toUpperCase(),
        side: document.getElementById('side').value,
        order_type: document.getElementById('orderType').value,
        price: parseFloat(document.getElementById('price').value) || 0,
        quantity: parseFloat(document.getElementById('quantity').value),
        time_in_force: document.getElementById('tif').value
    };

    try {
        const response = await fetch(`${API_BASE}/orders`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(orderData)
        });

        const result = await response.json();
        
        if (response.ok) {
            showStatus('Order placed successfully!', 'success');
            document.getElementById('orderForm').reset();
            document.getElementById('symbol').value = currentSymbol;
            
            // Refresh data
            setTimeout(() => {
                loadOrderBook();
                loadUserOrders();
            }, 500);
        } else {
            showStatus(result.error || 'Failed to place order', 'error');
        }
    } catch (error) {
        showStatus('Network error: ' + error.message, 'error');
    }
}

function showStatus(message, type) {
    const statusDiv = document.getElementById('orderStatus');
    statusDiv.textContent = message;
    statusDiv.className = `status-message ${type}`;
    
    setTimeout(() => {
        statusDiv.textContent = '';
        statusDiv.className = 'status-message';
    }, 3000);
}

async function loadOrderBook() {
    try {
        const response = await fetch(`${API_BASE}/orderbook/${currentSymbol}?limit=10`);
        const data = await response.json();
        
        displayOrderBook(data);
    } catch (error) {
        console.error('Failed to load order book:', error);
    }
}

function displayOrderBook(data) {
    const asksDiv = document.getElementById('asks');
    const bidsDiv = document.getElementById('bids');
    const spreadDiv = document.getElementById('spread');
    
    // Display asks (sorted high to low for display)
    asksDiv.innerHTML = '';
    if (data.asks && data.asks.length > 0) {
        const sortedAsks = [...data.asks].sort((a, b) => b.price - a.price);
        sortedAsks.forEach(ask => {
            const row = document.createElement('div');
            row.className = 'book-row';
            row.innerHTML = `
                <span class="ask">${ask.price.toFixed(2)}</span>
                <span>${ask.quantity.toFixed(4)}</span>
            `;
            asksDiv.appendChild(row);
        });
    }
    
    // Display bids (sorted high to low)
    bidsDiv.innerHTML = '';
    if (data.bids && data.bids.length > 0) {
        const sortedBids = [...data.bids].sort((a, b) => b.price - a.price);
        sortedBids.forEach(bid => {
            const row = document.createElement('div');
            row.className = 'book-row';
            row.innerHTML = `
                <span class="bid">${bid.price.toFixed(2)}</span>
                <span>${bid.quantity.toFixed(4)}</span>
            `;
            bidsDiv.appendChild(row);
        });
    }
    
    // Calculate and display spread
    if (data.asks && data.asks.length > 0 && data.bids && data.bids.length > 0) {
        const bestAsk = Math.min(...data.asks.map(a => a.price));
        const bestBid = Math.max(...data.bids.map(b => b.price));
        const spread = (bestAsk - bestBid).toFixed(2);
        spreadDiv.innerHTML = `Spread: <span>${spread}</span>`;
    }
}

async function loadRecentTrades() {
    try {
        const response = await fetch(`${API_BASE}/trades/${currentSymbol}?limit=20`);
        const trades = await response.json();
        
        displayTrades(trades);
    } catch (error) {
        console.error('Failed to load trades:', error);
    }
}

function displayTrades(trades) {
    const tradesDiv = document.getElementById('tradesList');
    tradesDiv.innerHTML = '';
    
    if (!trades || trades.length === 0) {
        tradesDiv.innerHTML = '<div style="padding: 20px; text-align: center; color: #8b949e;">No trades yet</div>';
        return;
    }
    
    trades.forEach(trade => {
        const row = document.createElement('div');
        row.className = `trade-row trade-${trade.side.toLowerCase()}`;
        
        const time = new Date(trade.timestamp).toLocaleTimeString();
        
        row.innerHTML = `
            <span>${trade.price.toFixed(2)}</span>
            <span>${trade.quantity.toFixed(4)}</span>
            <span>${time}</span>
        `;
        tradesDiv.appendChild(row);
    });
}

async function loadUserOrders() {
    try {
        const response = await fetch(`${API_BASE}/orders?user_id=${currentUser}`);
        const data = await response.json();
        
        displayUserOrders(data.orders || []);
    } catch (error) {
        console.error('Failed to load user orders:', error);
    }
}

function displayUserOrders(orders) {
    const ordersDiv = document.getElementById('userOrdersList');
    
    if (!orders || orders.length === 0) {
        ordersDiv.innerHTML = '<div style="padding: 20px; text-align: center; color: #8b949e;">No orders</div>';
        return;
    }
    
    let html = `
        <table class="orders-table">
            <thead>
                <tr>
                    <th>Order ID</th>
                    <th>Symbol</th>
                    <th>Side</th>
                    <th>Type</th>
                    <th>Price</th>
                    <th>Quantity</th>
                    <th>Filled</th>
                    <th>Status</th>
                    <th>Time</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    orders.forEach(order => {
        const latestStatus = order.status_history && order.status_history.length > 0 
            ? order.status_history[order.status_history.length - 1].status 
            : 'UNKNOWN';
        
        const statusClass = getStatusClass(latestStatus);
        const time = new Date(order.timestamp).toLocaleTimeString();
        
        html += `
            <tr>
                <td>${order.order_id.substring(0, 8)}...</td>
                <td>${order.symbol}</td>
                <td>${order.side}</td>
                <td>${order.order_type}</td>
                <td>${order.price > 0 ? order.price.toFixed(2) : 'Market'}</td>
                <td>${order.original_qty.toFixed(4)}</td>
                <td>${order.filled_qty.toFixed(4)}</td>
                <td><span class="status-badge ${statusClass}">${latestStatus}</span></td>
                <td>${time}</td>
            </tr>
        `;
    });
    
    html += '</tbody></table>';
    ordersDiv.innerHTML = html;
}

function getStatusClass(status) {
    const statusMap = {
        'NEW': 'status-new',
        'FILLED': 'status-filled',
        'PARTIALLY_FILLED': 'status-partial',
        'CANCELLED': 'status-cancelled'
    };
    return statusMap[status] || 'status-new';
}

function connectWebSocket() {
    ws = new WebSocket(WS_URL);
    
    ws.onopen = () => {
        console.log('WebSocket connected');
    };
    
    ws.onmessage = (event) => {
        const notification = JSON.parse(event.data);
        handleNotification(notification);
    };
    
    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
    };
    
    ws.onclose = () => {
        console.log('WebSocket disconnected, reconnecting...');
        setTimeout(connectWebSocket, 3000);
    };
}

function handleNotification(notification) {
    console.log('Notification received:', notification);
    
    // If it's a trade notification, refresh order book and trades
    if (notification.type === 'TRADE') {
        loadOrderBook();
        loadRecentTrades();
        loadUserOrders();
    }
    
    // If it's an order update for current user, refresh orders
    if (notification.user_id === currentUser) {
        loadUserOrders();
    }
}
