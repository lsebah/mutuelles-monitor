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
        loadActivity();
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
    const total = Object.values(byType).reduce((a,b) => a+b, 0);
    const spYes = (stats.structured_products || {}).yes || 0;
    const typeCards = Object.keys(byType).sort((a,b) => byType[b]-byType[a]).map(t => `
        <div class="type-card" onclick="filterByType('${t}')">
            <div class="type-count">${byType[t].toLocaleString('fr-FR')}</div>
            <div class="type-name">${labels[t] || t}</div>
        </div>
    `).join('');
    tg.innerHTML = `<div class="type-card type-card-total" onclick="statFilter('all')">
        <div class="type-count">${total.toLocaleString('fr-FR')}</div>
        <div class="type-name">Total entites</div>
    </div>` + typeCards + `<div class="type-card type-card-structured" onclick="statFilter('structured')">
        <div class="type-count">${spYes}</div>
        <div class="type-name">Porteurs structures</div>
    </div>`;

    const gg = document.getElementById('groupeGrid');
    const byGroupe = stats.by_groupe || {};
    const grpEntries = Object.entries(byGroupe).sort((a,b) => b[1]-a[1]).slice(0, 14);
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

// ---------- Stat card clicks ----------
function statFilter(key) {
    // Reset all filters first
    document.getElementById('searchInput').value = '';
    document.getElementById('filterType').value = '';
    document.getElementById('filterGroupe').value = '';
    document.getElementById('filterDepartment').value = '';
    document.getElementById('filterStructured').value = '';
    document.getElementById('filterHideFolk').checked = false;

    // Switch to directory tab
    document.querySelector('[data-tab="directory"]').click();

    switch (key) {
        case 'all':
            break; // no filter — show all
        case 'mutuelle':
            document.getElementById('filterType').value = 'mutuelle';
            break;
        case 'assurance':
            // Show all assurance types (vie + non-vie + mixte)
            document.getElementById('searchInput').value = '';
            // We'll use a custom approach: set a temp filter
            document.getElementById('filterType').value = 'assurance_vie';
            // Actually, better: just leave type empty and use search
            document.getElementById('filterType').value = '';
            break;
        case 'institution_prevoyance':
            document.getElementById('filterType').value = 'institution_prevoyance';
            break;
        case 'structured':
            document.getElementById('filterStructured').value = 'yes';
            break;
        case 'folk':
            // Show only folk-marked
            document.getElementById('filterHideFolk').checked = false;
            break;
    }

    // For 'assurance' we need a special filter since it spans 3 types
    if (key === 'assurance') {
        _statFilterOverride = 'assurance';
    } else if (key === 'folk') {
        _statFilterOverride = 'folk';
    } else {
        _statFilterOverride = null;
    }
    renderDirectory();
}
var _statFilterOverride = null;

// ---------- Directory ----------
function getFilteredEntities() {
    const search = (document.getElementById('searchInput')?.value || '').toLowerCase();
    const type = document.getElementById('filterType')?.value || '';
    const groupe = document.getElementById('filterGroupe')?.value || '';
    const dept = document.getElementById('filterDepartment')?.value || '';
    const sp = document.getElementById('filterStructured')?.value || '';
    const hideFolk = document.getElementById('filterHideFolk')?.checked || false;

    return allEntities.filter(e => {
        // Stat card overrides
        if (_statFilterOverride === 'assurance') {
            if (!['assurance_vie','assurance_non_vie','assurance_mixte'].includes(e.type_organisme)) return false;
        }
        if (_statFilterOverride === 'folk') {
            if (!isInFolk(e.id)) return false;
        }
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

    const typeClass = 'badge-type-' + (e.type_organisme || '');
    const badges = [
        `<span class="badge ${typeClass}">${typeLabel}</span>`,
    ];
    if (e.groupe) badges.push(`<span class="badge badge-groupe">${escHtml(e.groupe)}</span>`);
    if (spStatus === 'yes') badges.push('<span class="badge badge-structured-yes">PORTEUR STRUCTURES</span>');
    else if (spStatus === 'no') badges.push('<span class="badge badge-structured-no">non porteur</span>');
    else badges.push('<span class="badge badge-structured-unknown">a verifier</span>');

    const addr = e.address || {};
    const location = [addr.city, addr.department ? `(${addr.department})` : ''].filter(Boolean).join(' ');

    // Financial summary line - 4 KPIs: Primes, Résultat net, S/P, Rendement actifs
    let finSummary = '';
    if (e.financials) {
        const f = e.financials;
        const parts = [];
        if (f.primes_eur) parts.push(`<span class="fin-item"><span class="fin-label">Primes ${f.year || ''}:</span> <span class="fin-value">${formatEur(f.primes_eur)}</span></span>`);
        if (f.resultat_net_eur != null) {
            const cls = f.resultat_net_eur >= 0 ? 'fin-positive' : 'fin-negative';
            parts.push(`<span class="fin-item"><span class="fin-label">R. net:</span> <span class="fin-value ${cls}">${formatEur(f.resultat_net_eur)}</span></span>`);
        }
        if (f.sp_ratio != null) {
            const cls = f.sp_ratio > 100 ? 'fin-negative' : f.sp_ratio > 90 ? 'fin-warning' : 'fin-positive';
            parts.push(`<span class="fin-item"><span class="fin-label">S/P:</span> <span class="fin-value ${cls}">${f.sp_ratio.toFixed(1)}%</span></span>`);
        }
        if (f.rendement_actifs_pct != null) {
            const cls = f.rendement_actifs_pct >= 0 ? 'fin-positive' : 'fin-negative';
            parts.push(`<span class="fin-item"><span class="fin-label">Rdt actifs:</span> <span class="fin-value ${cls}">${f.rendement_actifs_pct >= 0 ? '+' : ''}${f.rendement_actifs_pct.toFixed(2)}%</span></span>`);
        }
        if (parts.length) finSummary = `<div class="entity-financials-bar">${parts.join('')}</div>`;
    }

    let peopleHtml = '';
    if (e.people && e.people.length) {
        peopleHtml = '<div class="entity-people"><div class="entity-people-title">Personnes cles</div><div class="entity-people-list">' +
            e.people.map(p => {
                const name = escHtml(p.name || '');
                const role = escHtml(p.role || '');
                const hasLinkedin = p.linkedin && p.linkedin.trim().startsWith('http');
                const linkedin = hasLinkedin
                    ? `<a class="name" href="${p.linkedin}" target="_blank" rel="noopener">${name} <span class="li-icon">in</span></a>`
                    : `<a class="name" href="https://www.linkedin.com/search/results/people/?keywords=${encodeURIComponent(p.name)}" target="_blank" rel="noopener" title="Rechercher sur LinkedIn">${name} <span class="li-icon">in</span></a>`;
                const email = p.email ? ` <a href="mailto:${p.email}" style="color:var(--accent-light-blue);font-size:11px">${p.email}</a>` : '';
                return `<div class="entity-people-row"><span class="role">${role}</span>${linkedin}${email}</div>`;
            }).join('') + '</div></div>';
    }

    // (financials now rendered as finSummary above)

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
                </div>
                ${finSummary}
                ${contact.length ? `<div class="entity-contact">${contact.join('')}</div>` : ''}
                ${peopleHtml}
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
    const header = ['Denomination','Type','Groupe','SIREN','Matricule','Adresse','CP','Ville','Departement','Region','Website','Email','Phone','Structures','Primes 2024','Resultat net','S/P %','Rdt actifs %','Personnes','Sources'];
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
            f.sp_ratio != null ? f.sp_ratio : '', f.rendement_actifs_pct != null ? f.rendement_actifs_pct : '',
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

// ---------- Activity Feed ----------
let activityEvents = [];
let activityShowAll = false;
const ACTIVITY_DAYS = 6;

async function loadActivity() {
    try {
        const resp = await fetch('data/activity.json');
        if (!resp.ok) return;
        const data = await resp.json();
        activityEvents = (data.events || []).sort((a, b) => b.date.localeCompare(a.date));
        renderActivityFeed();
    } catch (e) {
        console.warn('Activity feed not available:', e);
    }
}

function renderActivityFeed() {
    const feed = document.getElementById('activityFeed');
    const loadBtn = document.getElementById('activityLoadMore');
    if (!feed) return;
    if (!activityEvents.length) {
        feed.innerHTML = '<div class="empty-state"><p>Aucune activite enregistree pour le moment.</p><p>Les evenements apparaitront au fil des enrichissements.</p></div>';
        loadBtn.style.display = 'none';
        return;
    }

    // Group by date
    const byDate = new Map();
    for (const evt of activityEvents) {
        const d = evt.date;
        if (!byDate.has(d)) byDate.set(d, []);
        byDate.get(d).push(evt);
    }

    const dates = Array.from(byDate.keys()).sort().reverse();
    const visibleDates = activityShowAll ? dates : dates.slice(0, ACTIVITY_DAYS);
    const today = new Date().toISOString().slice(0, 10);
    const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);

    let html = '';
    for (const date of visibleDates) {
        const events = byDate.get(date);
        let label = date;
        if (date === today) label = "Aujourd'hui";
        else if (date === yesterday) label = 'Hier';
        else {
            const d = new Date(date + 'T12:00:00');
            label = d.toLocaleDateString('fr-FR', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' });
        }

        html += `<div class="activity-day">`;
        html += `<div class="activity-day-header">${label} <span class="activity-day-count">${events.length} evenement${events.length > 1 ? 's' : ''}</span></div>`;
        for (const evt of events) {
            const dotClass = {
                financial_update: 'dot-financial',
                person_joined: 'dot-person-joined',
                person_left: 'dot-person-left',
                new_entity: 'dot-new-entity',
                structured_update: 'dot-structured',
                entity_removed: 'dot-person-left',
            }[evt.type] || 'dot-financial';

            const typeLabel = {
                financial_update: 'Financier',
                person_joined: 'Arrivee',
                person_left: 'Depart',
                new_entity: 'Nouvelle entite',
                structured_update: 'Produits structures',
                entity_removed: 'Suppression',
            }[evt.type] || evt.type;

            html += `<div class="activity-event">
                <span class="activity-dot ${dotClass}"></span>
                <span class="activity-type-label">${typeLabel}</span>
                <span class="activity-entity" onclick="goToEntity('${evt.entity_id}')">${escHtml(evt.entity_name)}</span>
                <span class="activity-text">${escHtml(evt.summary)}</span>
            </div>`;
        }
        html += `</div>`;
    }

    feed.innerHTML = html;
    loadBtn.style.display = dates.length > ACTIVITY_DAYS && !activityShowAll ? 'block' : 'none';
}

function showFullHistory() {
    activityShowAll = true;
    renderActivityFeed();
}

function goToEntity(entityId) {
    document.querySelector('[data-tab="directory"]').click();
    const entity = allEntities.find(e => e.id === entityId);
    if (entity) {
        document.getElementById('searchInput').value = entity.denomination;
        _statFilterOverride = null;
        renderDirectory();
    }
}

// ---------- Tabs ----------
function renderCurrentTab() {
    const active = document.querySelector('.tab.active');
    if (!active) return;
    switch (active.dataset.tab) {
        case 'activity': renderActivityFeed(); break;
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
    if (el) el.addEventListener(el.tagName === 'INPUT' && el.type === 'text' ? 'input' : 'change', () => {
        _statFilterOverride = null; // clear stat card override on manual filter
        renderDirectory();
    });
});
document.getElementById('filterHideFolk')?.addEventListener('change', renderDirectory);

loadData();
