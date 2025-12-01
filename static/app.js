async function fetchDocs() {
  const q = document.getElementById("q").value.toLowerCase();
  const res = await fetch("/api/documents?limit=200");
  const docs = await res.json();
  const tbody = document.querySelector("#results tbody");
  tbody.innerHTML = "";
  for (const d of docs) {
    const title = d.title || "";
    const summary = d.summary || "";
    const found = q === "" || title.toLowerCase().includes(q) || (d.url && d.url.toLowerCase().includes(q));
    if (!found) continue;
    const tr = document.createElement("tr");
    const date = new Date(d.discovered_at);
    tr.innerHTML = `
      <td>${date.toISOString().replace('T',' ').split('.')[0]} UTC</td>
      <td>${escapeHtml(title)}</td>
      <td>${escapeHtml(summary || "")}</td>
      <td><a class="linkbtn" href="${d.url}" target="_blank">Abrir</a></td>
    `;
    tbody.appendChild(tr);
  }
}

function escapeHtml(s){return s ? s.replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])) : ""}

document.getElementById("refresh").addEventListener("click", fetchDocs);
document.getElementById("q").addEventListener("input", fetchDocs);

fetchDocs();
