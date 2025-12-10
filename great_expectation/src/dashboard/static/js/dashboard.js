const dataQualityMetrics = {
    tables: [
        {
            name: "table1",
            columns: [
                { name: "column1", quality: 98 },
                { name: "column2", quality: 95 },
                { name: "column3", quality: 99 }
            ]
        },
        {
            name: "table2",
            columns: [
                { name: "column1", quality: 97 },
                { name: "column2", quality: 99 },
                { name: "column3", quality: 98 }
            ]
        }
    ]
};

function renderDashboard() {
    const dashboardContainer = document.getElementById("dashboard");
    let html = "<h1>Data Quality Dashboard</h1>";

    dataQualityMetrics.tables.forEach(table => {
        html += `<h2>${table.name}</h2>`;
        html += "<ul>";
        table.columns.forEach(column => {
            html += `<li>${column.name}: ${column.quality}%</li>`;
        });
        html += "</ul>";
    });

    dashboardContainer.innerHTML = html;
}

document.addEventListener("DOMContentLoaded", renderDashboard);