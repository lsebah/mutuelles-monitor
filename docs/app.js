/**
 * Mutuelles Monitor - Annuaire frontend
 */
const FOLK_KEY = 'mutuelles-folk';
const PAGE_SIZE = 50;

let allEntities = [];
let typesLabels = {};
let displayOffset = 0;

// ---------- Folk tracker (localStorage) ----------
function getFolk() {
    try { return JSON.parse(localStorage.getItem(FOLK_KEY)) || {}; }
    catch { return {}; }
}
function isInFolk(id) { return !!getFolk()[id]; }
function toggleFolk(id) {
    const f = getFolk();
    if (f[id]) delete f[id];
    else f[id] = new Date().toISOString().slice(0, 10);
    localStorage.setItem(FOLK_KEY, JSON.stringify(f));
    updateFolkStat();
    renderCurrentTab();
}
function getFolkCount() { return Object.keys(getFolk()).length; }
function updateFolkStat() {
    const el = document.getElementById('statFolk');
    if (el) el.textContent = getFolkCount();
}

// ---------- Data loading ----------
async function loadData() {
    try {
        const resp = await fetch('data/entities.json');
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const data = await resp.json();
        allEntities = data.entities || [];
        typesLabels = data.types_labels || {};
        const stats = data.stats || {};

        document.getElementById('statTotal').textContent = stats.total || allEntities.length;
        const byType = stats.by_type || {};
        document.getElementById('statMutuelles').textContent = byType.mutuelle || 0;
        document.getElementById('statAssurances').textContent =
            (byType.assurance_vie || 0) + (byType.assurance_non_vie || 0) + (byType.assurance_mixte || 0);
        document.getElementById('statIP').textContent = byType.institution_prevoyance || 0;
        document.getElementById('statStructured').textContent = (stats.structured_products || {}).yes || 0;

        if (data.last_updated) {
            const d = new Date(data.last_updated);
            document.getElementById('lastUpdate').textContent =
                'Mis a jour: ' + d.toLocaleDateString('fr-FR') + ' ' + d.toLocaleTimeString('fr-FR', {hour:'2-digit',minute:'2-digit'});
        }

        // Populate dept filter
        const depts = new Map();
        allEntities.forEach(e => {
            const d = e.address && e.address.department;
            const dn = e.address && e.address.department_name;
            if (d) depts.set(d, dn || d);
        });
        const deptSel = document.getElementById('filterDepartment');
        Array.from(depts.entries()).sort().forEach(([code, name]) => {
            const opt = document.createElement('option');
            opt.value = code;
            opt.textContent = code + ' - ' + name;
            deptSel.appendChild(opt);
        });

        // Populate groupe filter
        const groupes = new Set();
        allEntities.forEach(e => { if (e.groupe) groupes.add(e.groupe); });
        const grpSel = document.getElementById('filterGroupe');
        Array.from(groupes).sort().forEach(g => {
            const opt = document.createElement('option');
            opt.value = g; opt.textContent = g;
            grpSel.appendChild(opt);
        });

        document.getElementById('badgeTotal').textContent = allEntities.length;
        document.getElementById('badgeStructured').textContent = (stats.structured_products || {}).yes || '';
        document.getElementById('badgeFederations').textContent = byType.federation || '';

        renderDashboard(stats);
        renderDirectory();
        renderStructured();
        renderFederations();
        updateFolkStat();
    } catch (e) {
        console.warn('Error loading data:', e);
        document.getElementById('lastUpdate').textContent = 'Aucune donnee. Lance: python scraper/main.py --bootstrap';
    }
}

// ---------- Dashboard ----------
function renderDashboard(stats) {
    const tg = document.getElementById('typeGrid');
    const byType = stats.by_type || {};
    const labels = typesLabels;
    tg.innerHTML = Object.keys(byType).sort((a,b) => byType[b]-byType[a]).map(t => `
        <div class="type-card" onclick="filterByType('${t}')">
            <div class="type-name">${labels[t] || t}</div>
            <div class="type-count">${byType[t].toLocaleString('fr-FR')}</div>
            <div class="type-label">entites</div>
        </div>
    `).join('');

    const gg = document.getElementById('groupeGrid');
    const byGroupe = stats.by_groupe || {};
    const grpEntries = Object.entries(byGroupe).sort((a,b) => b[1]-a[1]).slice(0, 12);
    gg.innerHTML = grpEntries.length
        ? grpEntries.map(([g, c]) => `
            <div class="type-card" onclick="filterByGroupe('${g.replace(/'/g, "\\'")}')">
                <div class="type-name">${g}</div>
                <div class="type-count">${c}</div>
                <div class="type-label">filiales</div>
            </div>
        `).join('')
        : '<div class="empty-state">Aucun groupe taggue. Lance le scraper.</div>';
}

function filterByType(t) {
    document.querySelector('[data-tab="directory"]').click();
    document.getElementById('filterType').value = t;
    renderDirectory();
}
function filterByGroupe(g) {
    document.querySelector('[data-tab="directory"]').click();
    document.getElementById('filterGroupe').value = g;
    renderDirectory();
}

// ---------- Directory ----------
function getFilteredEntities() {
    const search = (document.getElementById('searchInput')?.value || '').toLowerCase();
    const type = document.getElementById('filterType')?.value || '';
    const groupe = document.getElementById('filterGroupe')?.value || '';
    const dept = document.getElementById('filterDepartment')?.value || '';
    const sp = document.getElementById('filterStructured')?.value || '';
    const hideFolk = document.getElementById('filterHideFolk')?.checked || false;

    return allEntities.filter(e => {
        if (hideFolk && isInFolk(e.id)) return false;
        if (type && e.type_organisme !== type) return false;
        if (groupe && e.groupe !== groupe) return false;
        if (dept && (e.address?.department || '') !== dept) return false;
        if (sp && (e.structured_products?.status || 'unknown') !== sp) return false;
        if (search) {
            const hay = [
                e.denomination, e.siren, e.groupe,
                e.address?.city, e.address?.department_name,
                e.email, e.phone, e.website,
                ...(e.people || []).map(p => p.name),
            ].join(' ').toLowerCase();
            if (!hay.includes(search)) return false;
        }
        return true;
    });
}

function renderDirectory() {
    displayOffset = 0;
    const filtered = getFilteredEntities();
    const grid = document.getElementById('membersGrid');
    const countEl = document.getElementById('membersCount');
    const loadBtn = document.getElementById('loadMoreBtn');

    countEl.textContent = filtered.length.toLocaleString('fr-FR') + ' entites trouvees';
    const page = filtered.slice(0, PAGE_SIZE);
    grid.innerHTML = page.length
        ? page.map(renderEntityCard).join('')
        : '<div class="empty-state"><p>Aucun resultat</p></div>';
    displayOffset = PAGE_SIZE;
    loadBtn.style.display = filtered.length > PAGE_SIZE ? 'block' : 'none';
}

function loadMore() {
    const filtered = getFilteredEntities();
    const grid = document.getElementById('membersGrid');
    const loadBtn = document.getElementById('loadMoreBtn');
    const page = filtered.slice(displayOffset, displayOffset + PAGE_SIZE);
    grid.innerHTML += page.map(renderEntityCard).join('');
    displayOffset += PAGE_SIZE;
    loadBtn.style.display = displayOffset < filtered.length ? 'block' : 'none';
}

// ---------- Structured tab ----------
function renderStructured() {
    const grid = document.getElementById('structuredGrid');
    const yes = allEntities
        .filter(e => (e.structured_products?.status === 'yes'))
        .sort((a,b) => (b.financials?.primes_eur || 0) - (a.financials?.primes_eur || 0));
    if (!yes.length) {
        grid.innerHTML = '<div class="empty-state"><p>Aucun porteur identifie pour le moment.</p><p>Lance: python scraper/enrichment/sfcr_parser.py</p></div>';
        return;
    }
    grid.innerHTML = yes.map(renderEntityCard).join('');
}

// ---------- Federations tab ----------
function renderFederations() {
    const grid = document.getElementById('federationsGrid');
    const fed = allEntities.filter(e => e.type_organisme === 'federation');
    if (!fed.length) {
        grid.innerHTML = '<div class="empty-state"><p>Aucune federation chargee.</p><p>Lance: python scraper/main.py --refresh</p></div>';
        return;
    }
    grid.innerHTML = fed.map(renderEntityCard).join('');
}

// ---------- Card rendering ----------
function renderEntityCard(e) {
    const folk = isInFolk(e.id);
    const sp = e.structured_products || {};
    const spStatus = sp.status || 'unknown';
    const typeLabel = typesLabels[e.type_organisme] || e.type_organisme || '';

    const badges = [
        `<span class="badge badge-type">${typeLabel}</span>`,
    ];
    if (e.groupe) badges.push(`<span class="badge badge-groupe">${escHtml(e.groupe)}</span>`);
    if (spStatus === 'yes') badges.push('<span class="badge badge-structured-yes">PORTEUR STRUCTURES</span>');
    else if (spStatus === 'no') badges.push('<span class="badge badge-structured-no">non porteur</span>');
    else badges.push('<span class="badge badge-structured-unknown">a verifier</span>');

    const addr = e.address || {};
    const location = [addr.city, addr.department ? `(${addr.department})` : ''].filter(Boolean).join(' ');

    let peopleHtml = '';
    if (e.people && e.people.length) {
        peopleHtml = '<div class="entity-people"><div class="entity-people-title">Personnes cles</div><div class="entity-people-list">' +
            e.people.map(p => {
                const name = escHtml(p.name || '');
                const role = escHtml(p.role || '');
                const linkedin = p.linkedin
                    ? `<a class="name" href="${p.linkedin}" target="_blank" rel="noopener">${name} <span class="li-icon">in</span></a>`
                    : `<span class="name">${name}</span>`;
                const email = p.email ? ` <a href="mailto:${p.email}" style="color:var(--accent-light-blue);font-size:11px">${p.email}</a>` : '';
                return `<div class="entity-people-row"><span class="role">${role}</span>${linkedin}${email}</div>`;
            }).join('') + '</div></div>';
    }

    let finHtml = '';
    if (e.financials) {
        const primes = e.financials.primes_eur ? formatEur(e.financials.primes_eur) : '';
        const rn = e.financials.resultat_net_eur ? formatEur(e.financials.resultat_net_eur) : '';
        const yr = e.financials.year || '';
        finHtml = `<div class="entity-financials">
            ${primes ? `<span><span class="label">Primes ${yr}:</span> <span class="value">${primes}</span></span>` : ''}
            ${rn ? `<span><span class="label">Resultat net:</span> <span class="value">${rn}</span></span>` : ''}
        </div>`;
    }

    const contact = [];
    if (e.phone) contact.push(`<a href="tel:${e.phone}">${e.phone}</a>`);
    if (e.email) contact.push(`<a href="mailto:${e.email}">${e.email}</a>`);
    if (e.website) contact.push(`<a href="${e.website.startsWith('http') ? e.website : 'https://' + e.website}" target="_blank">${escHtml(e.website)}</a>`);

    const sources = Object.keys(e.sources || {});
    const cardClass = (folk ? ' is-folk' : '') + (spStatus === 'yes' ? ' has-structured' : '');

    return `
        <div class="entity-card${cardClass}">
            <div class="entity-info">
                <div class="entity-header">
                    <span class="entity-name">${escHtml(e.denomination)}</span>
                    ${badges.join('')}
                </div>
                <div class="entity-meta">
                    ${location ? `<span>${location}</span>` : ''}
                    ${addr.street ? `<span>${escHtml(addr.street)}</span>` : ''}
                    ${e.siren ? `<span>SIREN: ${e.siren}</span>` : ''}
                    ${e.matricule && e.matricule !== e.siren ? `<span>Mat: ${e.matricule}</span>` : ''}
                </div>
                ${contact.length ? `<div class="entity-contact">${contact.join('')}</div>` : ''}
                ${peopleHtml}
                ${finHtml}
            </div>
            <div class="entity-actions">
                <label class="contact-toggle" title="Marquer comme dans Folk">
                    <input type="checkbox" ${folk ? 'checked' : ''} onchange="toggleFolk('${e.id}')">
                    <span class="toggle-switch"></span>
                    Dans Folk
                </label>
                ${sources.length ? `<div class="source-tag">${sources.join(' · ')}</div>` : ''}
            </div>
        </div>
    `;
}

function formatEur(n) {
    if (typeof n !== 'number') return '';
    if (n >= 1e9) return (n / 1e9).toFixed(2) + ' Md€';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + ' M€';
    if (n >= 1e3) return (n / 1e3).toFixed(0) + ' k€';
    return n + ' €';
}

function escHtml(t) {
    const div = document.createElement('div');
    div.textContent = t == null ? '' : String(t);
    return div.innerHTML;
}

// ---------- CSV export ----------
function exportFilteredCsv() {
    const rows = getFilteredEntities();
    const header = ['Denomination','Type','Groupe','SIREN','Matricule','Adresse','CP','Ville','Departement','Region','Website','Email','Phone','Structures','Primes 2024','Resultat net','Personnes','Sources'];
    const csv = [header.join(';')];
    for (const e of rows) {
        const a = e.address || {};
        const f = e.financials || {};
        const sp = e.structured_products || {};
        const people = (e.people || []).map(p => `${p.role || ''}: ${p.name || ''}${p.linkedin ? ' (' + p.linkedin + ')' : ''}`).join(' | ');
        const row = [
            e.denomination, typesLabels[e.type_organisme] || e.type_organisme, e.groupe || '',
            e.siren || '', e.matricule || '',
            a.street || '', a.postal_code || '', a.city || '', a.department || '', a.region || '',
            e.website || '', e.email || '', e.phone || '',
            sp.status || '',
            f.primes_eur || '', f.resultat_net_eur || '',
            people,
            Object.keys(e.sources || {}).join(',')
        ].map(csvEscape);
        csv.push(row.join(';'));
    }
    const blob = new Blob(['\ufeff' + csv.join('\n')], {type:'text/csv;charset=utf-8'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `mutuelles-export-${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
}
function csvEscape(v) {
    if (v == null) return '';
    const s = String(v);
    if (s.includes(';') || s.includes('"') || s.includes('\n')) {
        return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
}

// ---------- Tabs ----------
function renderCurrentTab() {
    const active = document.querySelector('.tab.active');
    if (!active) return;
    switch (active.dataset.tab) {
        case 'directory': renderDirectory(); break;
        case 'structured': renderStructured(); break;
        case 'federations': renderFederations(); break;
    }
}

document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        tab.classList.add('active');
        document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
        renderCurrentTab();
    });
});

['searchInput','filterType','filterGroupe','filterDepartment','filterStructured'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener(el.tagName === 'INPUT' && el.type === 'text' ? 'input' : 'change', renderDirectory);
});
document.getElementById('filterHideFolk')?.addEventListener('change', renderDirectory);

loadData();
