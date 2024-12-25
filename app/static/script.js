document.getElementById('queryForm').addEventListener('submit', async (e) => {
    e.preventDefault(); // Prevent page reload
    const query = document.getElementById('query').value;
    const outputDiv = document.getElementById('output');
    outputDiv.innerHTML = 'Running query...';

    try {
        const response = await fetch('/api/query', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ query }),
        });

        const data = await response.json();

        if (response.ok) {
            outputDiv.innerHTML = `<pre>${JSON.stringify(data.results, null, 2)}</pre>`;
        } else {
            outputDiv.innerHTML = `<div class="error">Error: ${data.error}</div>`;
        }
    } catch (err) {
        outputDiv.innerHTML = `<div class="error">Error: ${err.message}</div>`;
    }
});
