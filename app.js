/* ============================================================
   FactureCheck — Sanit Climat
   Logique complète : Firebase, scan PDF/OCR, base articles,
   détection d'écarts, exports
   ============================================================ */

'use strict';

// PDF.js worker
pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';

// ---------- ÉTAT GLOBAL ----------
const STATE = {
  articles: {},      // { id: {code, designation, supplier, category, unit, price, tolerance, supplierCode, notes, updatedAt} }
  suppliers: {},     // { id: {name, code, siret, address, phone, email, payment, discount, notes} }
  invoices: {},      // { id: {supplier, number, date, total, lines:[], createdAt} }
  config: {
    firebase: null,  // { apiKey, authDomain, databaseURL, projectId }
    threshWarn: 2,
    threshDanger: 5,
  },
  fb: { app: null, db: null, connected: false },
  currentInvoice: null, // facture en cours d'extraction (avant save)
  pendingArticles: [],  // articles à valider après extraction
};

// ---------- LOCALSTORAGE KEYS ----------
const LS_KEY = 'facturecheck.v1';

// ============================================================
// PERSISTENCE — Firebase + localStorage fallback
// ============================================================

function loadLocal() {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return;
    const data = JSON.parse(raw);
    STATE.articles = data.articles || {};
    STATE.suppliers = data.suppliers || {};
    STATE.invoices = data.invoices || {};
    STATE.config = { ...STATE.config, ...(data.config || {}) };
  } catch(e) { console.warn('loadLocal', e); }
}

function saveLocal() {
  try {
    localStorage.setItem(LS_KEY, JSON.stringify({
      articles: STATE.articles,
      suppliers: STATE.suppliers,
      invoices: STATE.invoices,
      config: STATE.config,
    }));
  } catch(e) { console.warn('saveLocal', e); }
}

async function fbInit(cfg) {
  try {
    if (firebase.apps.length) firebase.app().delete().catch(()=>{});
    STATE.fb.app = firebase.initializeApp(cfg);
    STATE.fb.db = firebase.database();
    // Test connection
    await STATE.fb.db.ref('.info/connected').once('value');
    STATE.fb.connected = true;
    setFbStatus(true);
    // Pull all data
    const snap = await STATE.fb.db.ref('facturecheck').once('value');
    const remote = snap.val() || {};
    if (remote.articles) STATE.articles = remote.articles;
    if (remote.suppliers) STATE.suppliers = remote.suppliers;
    if (remote.invoices) STATE.invoices = remote.invoices;
    // Live listeners
    STATE.fb.db.ref('facturecheck/articles').on('value', s => {
      STATE.articles = s.val() || {};
      renderArticles(); renderSuppliersDatalists();
    });
    STATE.fb.db.ref('facturecheck/suppliers').on('value', s => {
      STATE.suppliers = s.val() || {};
      renderSuppliers(); renderSuppliersDatalists();
    });
    STATE.fb.db.ref('facturecheck/invoices').on('value', s => {
      STATE.invoices = s.val() || {};
      renderInvoices(); renderEcarts(); renderKPI();
    });
    saveLocal();
    return true;
  } catch(e) {
    console.error('fbInit', e);
    setFbStatus(false);
    return false;
  }
}

function fbDisconnect() {
  if (STATE.fb.app) { try { STATE.fb.app.delete(); } catch(e){} }
  STATE.fb = { app: null, db: null, connected: false };
  setFbStatus(false);
}

async function fbWrite(path, data) {
  if (STATE.fb.connected && STATE.fb.db) {
    try { await STATE.fb.db.ref('facturecheck/' + path).set(data); }
    catch(e) { console.warn('fbWrite', e); }
  }
  saveLocal();
}

async function fbRemove(path) {
  if (STATE.fb.connected && STATE.fb.db) {
    try { await STATE.fb.db.ref('facturecheck/' + path).remove(); }
    catch(e) { console.warn('fbRemove', e); }
  }
  saveLocal();
}

function setFbStatus(connected) {
  const dot = document.getElementById('fbDot');
  const txt = document.getElementById('fbStatus');
  if (connected) {
    dot.classList.add('online'); dot.classList.remove('offline');
    txt.textContent = 'Firebase';
  } else {
    dot.classList.remove('online'); dot.classList.add('offline');
    txt.textContent = 'Local';
  }
}

// ============================================================
// UTILITAIRES
// ============================================================

const uid = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
const $ = sel => document.querySelector(sel);
const $$ = sel => document.querySelectorAll(sel);
const fmt = n => (n == null || isNaN(n)) ? '—' : Number(n).toLocaleString('fr-FR', { minimumFractionDigits: 2, maximumFractionDigits: 4 });
const fmtMoney = n => (n == null || isNaN(n)) ? '—' : Number(n).toLocaleString('fr-FR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' €';
const fmtPct = n => (n == null || isNaN(n)) ? '—' : (n >= 0 ? '+' : '') + Number(n).toFixed(1) + ' %';
const fmtDate = d => { if (!d) return '—'; try { return new Date(d).toLocaleDateString('fr-FR'); } catch { return d; } };
const today = () => new Date().toISOString().slice(0, 10);

function toast(msg, type = '') {
  const t = $('#toast');
  t.className = 'toast show ' + (type ? 'toast-' + type : '');
  t.textContent = msg;
  clearTimeout(t._tm);
  t._tm = setTimeout(() => t.classList.remove('show'), 3500);
}

function normalize(s) {
  return (s || '').toString().toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^\w\s]/g, ' ').replace(/\s+/g, ' ').trim();
}

function parseFrNumber(s) {
  if (s == null) return NaN;
  if (typeof s === 'number') return s;
  // "1 234,56" -> 1234.56  ; "1.234,56" -> 1234.56  ; "1234.56" -> 1234.56
  let v = String(s).trim().replace(/\s/g, '').replace(/€/g, '');
  // détecter format
  if (v.includes(',') && v.includes('.')) {
    // si la virgule est après le point => format FR (1.234,56)
    if (v.lastIndexOf(',') > v.lastIndexOf('.')) v = v.replace(/\./g, '').replace(',', '.');
    else v = v.replace(/,/g, '');
  } else if (v.includes(',')) {
    v = v.replace(',', '.');
  }
  const n = parseFloat(v);
  return isNaN(n) ? NaN : n;
}

// ============================================================
// EXTRACTION PDF / OCR
// ============================================================

async function extractFromFile(file) {
  showOcrProgress(true, 'Lecture du fichier…', 5);
  let text = '';
  try {
    if (file.type === 'application/pdf' || file.name.toLowerCase().endsWith('.pdf')) {
      text = await extractTextFromPdf(file);
      // Si pas de texte, fallback OCR sur images du PDF
      if (text.trim().length < 50) {
        showOcrProgress(true, 'PDF scanné détecté, OCR en cours…', 30);
        text = await ocrPdfPages(file);
      }
    } else {
      // image
      showOcrProgress(true, 'OCR en cours…', 20);
      text = await ocrImage(file);
    }
  } catch (e) {
    console.error(e);
    toast('Erreur d\'extraction : ' + e.message, 'danger');
  }
  showOcrProgress(false);
  return text;
}

async function extractTextFromPdf(file) {
  const buf = await file.arrayBuffer();
  const pdf = await pdfjsLib.getDocument({ data: buf }).promise;
  let full = '';
  for (let i = 1; i <= pdf.numPages; i++) {
    const page = await pdf.getPage(i);
    const content = await page.getTextContent();
    const lines = {};
    content.items.forEach(it => {
      const y = Math.round(it.transform[5]);
      if (!lines[y]) lines[y] = [];
      lines[y].push({ x: it.transform[4], str: it.str });
    });
    const ys = Object.keys(lines).map(Number).sort((a, b) => b - a);
    ys.forEach(y => {
      const line = lines[y].sort((a, b) => a.x - b.x).map(o => o.str).join(' ').replace(/\s+/g, ' ').trim();
      if (line) full += line + '\n';
    });
    full += '\n';
    showOcrProgress(true, `Page ${i}/${pdf.numPages}`, 5 + (i / pdf.numPages) * 25);
  }
  return full;
}

async function ocrPdfPages(file) {
  const buf = await file.arrayBuffer();
  const pdf = await pdfjsLib.getDocument({ data: buf }).promise;
  let full = '';
  for (let i = 1; i <= pdf.numPages; i++) {
    const page = await pdf.getPage(i);
    const viewport = page.getViewport({ scale: 2 });
    const canvas = document.createElement('canvas');
    canvas.width = viewport.width; canvas.height = viewport.height;
    const ctx = canvas.getContext('2d');
    await page.render({ canvasContext: ctx, viewport }).promise;
    const dataUrl = canvas.toDataURL();
    showOcrProgress(true, `OCR page ${i}/${pdf.numPages}…`, 30 + (i / pdf.numPages) * 60);
    const r = await Tesseract.recognize(dataUrl, 'fra', {
      logger: m => {
        if (m.status === 'recognizing text') {
          const p = 30 + ((i - 1) / pdf.numPages) * 60 + (m.progress / pdf.numPages) * 60;
          showOcrProgress(true, `OCR page ${i}/${pdf.numPages} — ${Math.round(m.progress * 100)}%`, p);
        }
      }
    });
    full += r.data.text + '\n\n';
  }
  return full;
}

async function ocrImage(file) {
  const url = URL.createObjectURL(file);
  const r = await Tesseract.recognize(url, 'fra', {
    logger: m => {
      if (m.status === 'recognizing text') {
        showOcrProgress(true, `OCR — ${Math.round(m.progress * 100)}%`, 20 + m.progress * 70);
      }
    }
  });
  URL.revokeObjectURL(url);
  return r.data.text;
}

function showOcrProgress(show, label, pct) {
  const w = $('#ocrProgress');
  if (!show) { w.hidden = true; return; }
  w.hidden = false;
  $('#ocrLabel').textContent = label || '';
  $('#ocrBar').style.width = (pct || 0) + '%';
}

// ============================================================
// PARSING DE FACTURE
// ============================================================

const SUPPLIER_PATTERNS = [
  // --- Grossistes / négoces matériaux ---
  { re: /\baredis\b/i, name: 'Aredis Robinetterie' },
  { re: /\bcedeo\b/i, name: 'Cedeo' },
  { re: /\bbrossette\b/i, name: 'Brossette' },
  { re: /\brexel\b/i, name: 'Rexel' },
  { re: /\bw[uü]rth\b/i, name: 'Würth' },
  { re: /point\s*p\b/i, name: 'Point P' },
  { re: /saint[\s-]*gobain/i, name: 'Saint-Gobain' },
  { re: /\bsonepar\b/i, name: 'Sonepar' },
  { re: /yesss\s*[ée]lectrique/i, name: 'Yesss Électrique' },
  { re: /\bcastorama\b/i, name: 'Castorama' },
  { re: /leroy\s*merlin/i, name: 'Leroy Merlin' },
  { re: /\bprolians\b/i, name: 'Prolians' },
  { re: /\btereva\b/i, name: 'Tereva' },
  { re: /frans\s*bonhomme/i, name: 'Frans Bonhomme' },
  { re: /\brichardson\b/i, name: 'Richardson' },
  { re: /\bpum\b/i, name: 'PUM' },
  { re: /\bbruneau\b/i, name: 'Bruneau' },
  // --- Logiciels / services ---
  { re: /bigchange|big\s*change/i, name: 'BigChange' },
  { re: /astbtp|ast\s*btp|services?\s*sant[ée]\s*au\s*travail/i, name: 'ASTBTP 13' },
];

// Détection nom fournisseur quand pas dans la liste : prendre la 1ère ligne en MAJUSCULES significative,
// ou un pattern style "Nom Prénom" en début, ou ce qui suit "Émetteur"
function autoDetectSupplier(text) {
  const lines = text.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  const blacklist = /^(reference|référence|article|description|d[ée]signation|qte|qt[ée]|quantit[ée]|prix|unitaire|montant|total|tva|ht|ttc|net|page|adresse|t[ée]l|fax|email|siret|num[ée]ro|code|client|date|conditions?|paiement|virement|facture|invoice|chantier|commande|n[°o]|montant|hors\s*taxe|soumis|taux|garantie|p[ée]riode|exigible|adh[ée]rent|sasu|sas|sarl|sa|ei|eurl|sci|cba?|ttc|hors\s+taxes?|france|amount|vat|company|reg|tel)$/i;
  // Si la ligne contient PLUSIEURS mots-clés tableau → c'est un en-tête, pas un nom de société
  const isTableHeaderLine = (s) => {
    const lc = s.toLowerCase();
    let hits = 0;
    for (const kw of ['reference', 'référence', 'article', 'description', 'designation', 'désignation', 'quantité', 'quantite', 'qte', 'qté', 'prix', 'montant', 'total', 'unité', 'unite', 'unit', 'tva', 'ttc', 'pu ', 'p.u', 'garantie', 'taxe']) {
      if (lc.includes(kw)) hits++;
      if (hits >= 2) return true;
    }
    return false;
  };

  // 1. Cherche après le mot "Émetteur" / "Emetteur" / "De" / "From"
  for (let i = 0; i < lines.length; i++) {
    if (/^(émetteur|emetteur|expéditeur|from|de\s*:)/i.test(lines[i])) {
      for (let j = i + 1; j < Math.min(i + 5, lines.length); j++) {
        const cand = lines[j];
        if (/^(soci[ée]t[ée]|adresse|pays|num[ée]ro|code|tva|n[°o]|votre\s+contact)\s*:?\s*$/i.test(cand)) continue;
        if (cand.length > 2 && cand.length < 50 && !/^\d/.test(cand) && !/^:/.test(cand) && !blacklist.test(cand) && !isTableHeaderLine(cand)) {
          return cand.trim();
        }
      }
    }
  }

  // 2. Cherche un nom de société avec forme légale
  for (const l of lines.slice(0, 30)) {
    const m = l.match(/^([A-ZÀ-Ÿ][\wÀ-ÿ' \-&\.]{2,40})\s+(SAS|SASU|SARL|SA|EI|EURL|SCI|SCEA|GAEC|SCOP|LIMITED|LTD|GMBH)\b/);
    if (m && !blacklist.test(m[1]) && !isTableHeaderLine(m[1])) return (m[1] + ' ' + m[2]).trim();
  }

  // 3. Première ligne courte non-vide, en grosse partie majuscules (mais pas un mot-clé de tableau)
  for (const l of lines.slice(0, 10)) {
    if (l.length < 3 || l.length > 40) continue;
    if (/facture|invoice|n[°o]\s*facture/i.test(l)) continue;
    if (/^[\d\W]/.test(l)) continue;
    if (blacklist.test(l)) continue;
    if (isTableHeaderLine(l)) continue;
    const upper = (l.match(/[A-ZÀ-Ÿ]/g) || []).length;
    const lower = (l.match(/[a-zà-ÿ]/g) || []).length;
    if (upper > lower && upper > 2) return l.trim();
  }

  return '';
}

// --- Parsing date avec mois en lettres ---
const FR_MONTHS = {
  'janvier': 1, 'janv': 1, 'jan': 1,
  'fevrier': 2, 'février': 2, 'fevr': 2, 'fev': 2, 'févr': 2, 'fév': 2,
  'mars': 3, 'mar': 3,
  'avril': 4, 'avr': 4,
  'mai': 5,
  'juin': 6,
  'juillet': 7, 'juil': 7,
  'aout': 8, 'août': 8,
  'septembre': 9, 'sept': 9, 'sep': 9,
  'octobre': 10, 'oct': 10,
  'novembre': 11, 'nov': 11,
  'decembre': 12, 'décembre': 12, 'dec': 12, 'déc': 12,
};
const EN_MONTHS = {
  'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
  'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
  'august': 8, 'aug': 8, 'september': 9, 'sep': 9, 'sept': 9,
  'october': 10, 'oct': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12,
};

function tryParseDate(s) {
  if (!s) return '';
  s = s.trim();

  // Format avec mois en lettres : "07 Avril 2026", "22 avril 2026", "06 avril 2026"
  let m = s.match(/(\d{1,2})\s+([a-zA-Zéûôâèà]+)\.?\s+(\d{2,4})/);
  if (m) {
    const day = parseInt(m[1]);
    const monthName = m[2].toLowerCase().replace(/[éèê]/g, 'e').replace(/[àâ]/g, 'a').replace(/[ûù]/g, 'u');
    const month = FR_MONTHS[monthName] || EN_MONTHS[monthName];
    let year = parseInt(m[3]);
    if (year < 100) year += year > 50 ? 1900 : 2000;
    if (month && day >= 1 && day <= 31 && year >= 1990 && year <= 2099) {
      return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    }
  }

  // Format numérique : tolère / - . et espaces (ex : "02 - 03 - 2026", "30.03.2026")
  m = s.match(/(\d{1,2})\s*[\/\-\.]\s*(\d{1,2})\s*[\/\-\.]\s*(\d{2,4})/);
  if (m) {
    let d = parseInt(m[1]), mo = parseInt(m[2]), y = parseInt(m[3]);
    if (y < 100) y += y > 50 ? 1900 : 2000;
    // Détecter si format inverse (YYYY-MM-DD)
    if (d > 31) { [d, y] = [y, d]; }
    if (d >= 1 && d <= 31 && mo >= 1 && mo <= 12 && y >= 1990 && y <= 2099) {
      return `${y}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
    }
  }

  // ISO : 2026-04-15
  m = s.match(/(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (m) return `${m[1]}-${String(m[2]).padStart(2,'0')}-${String(m[3]).padStart(2,'0')}`;

  return '';
}

// Cherche des dates dans un texte selon différents libellés
// Le label peut être suivi de la date sur la même ligne OU sur les lignes suivantes
function findDateAfter(text, labels) {
  const lines = text.split(/\r?\n/);
  for (const label of labels) {
    const labelRe = new RegExp('(?:^|\\b)' + label + '\\b', 'i');
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (!labelRe.test(line)) continue;
      if (LINE_NOISE_RE.test(line)) continue; // ignorer "Décret 2009-138..." etc.
      // 1. Sur la même ligne après le label
      const afterLabel = line.replace(labelRe, '|||LABEL|||').split('|||LABEL|||')[1] || '';
      if (afterLabel) {
        const d = tryExtractDate(afterLabel);
        if (d) return d;
      }
      // 2. Sur les 3 lignes suivantes (cas où la valeur est en dessous)
      for (let j = i + 1; j < Math.min(i + 4, lines.length); j++) {
        if (LINE_NOISE_RE.test(lines[j])) continue;
        const d = tryExtractDate(lines[j]);
        if (d) return d;
      }
    }
  }
  return '';
}

function tryExtractDate(s) {
  // Trouver une date dans une chaîne (premier match)
  let m = s.match(/(\d{1,2}\s*[\/\-\.]\s*\d{1,2}\s*[\/\-\.]\s*\d{2,4})/);
  if (m) return tryParseDate(m[1]);
  m = s.match(/(\d{1,2}\s+(?:janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|sept|octobre|octobre|novembre|décembre|decembre|january|february|march|april|may|june|july|august|september|october|november|december|janv|févr|fevr|fév|fev|jan|feb|mar|apr|jun|jul|aug|oct|nov|dec)\.?\s+\d{2,4})/i);
  if (m) return tryParseDate(m[1]);
  m = s.match(/(\d{4}-\d{1,2}-\d{1,2})/);
  if (m) return tryParseDate(m[1]);
  return '';
}

// Calcule une échéance "30 jours" depuis une date
function applyRelativeDue(invoiceDate, text) {
  if (!invoiceDate) return '';
  const lc = text.toLowerCase();
  let days = null;
  let endOfMonth = false;
  let m = lc.match(/(\d{1,3})\s*jours?\s*(?:fin\s*de\s*mois|fdm)/i);
  if (m) { days = parseInt(m[1]); endOfMonth = true; }
  else {
    m = lc.match(/(?:payment\s*terms|conditions?\s*de\s*r[èe]glement|d[ée]lai|net)\s*[:.]?\s*(\d{1,3})\s*(?:jours|days|j\b)/i);
    if (m) days = parseInt(m[1]);
    else {
      m = lc.match(/\b(\d{1,3})\s*jours?\s*(?:net|nets)?\s*(?:date\s*de\s*facture)?/i);
      if (m) days = parseInt(m[1]);
    }
  }
  if (days == null) return '';
  const d = new Date(invoiceDate);
  d.setDate(d.getDate() + days);
  if (endOfMonth) d.setMonth(d.getMonth() + 1, 0);
  return d.toISOString().slice(0, 10);
}

// Tronque le texte aux zones CGV / mentions légales / footer pour éviter les faux positifs.
// Important : on ne coupe PAS sur les mentions courtes type "(Décret ...)" ni "selon les articles
// du code général" car celles-ci apparaissent souvent en plein milieu du contenu utile.
// On ne coupe que sur les marqueurs de blocs CGV évidents qui terminent le doc.
function trimFooter(text) {
  const cuts = [
    // Le titre doit être majeur (en début de ligne ou seul) pour éviter de couper sur
    // "Retrouver nos conditions générales de ventes sur notre site internet..."
    /(?:^|\n)\s*CONDITIONS\s+G[ÉE]N[ÉE]RALES?\s+DE\s+VENTE\s*$/im,
    /(?:^|\n)\s*\bMENTIONS?\s+L[ÉE]GALES\b\s*$/im,
    /^Cl?ause\s+attributive/im,
    /R[ée]serves?\s+de\s+propri[ée]t[ée]\s*:/i,
    /Catalogue,?\s+documentations?\s+et\s+propri[ée]t[ée]/i,
  ];
  let cutIdx = text.length;
  for (const re of cuts) {
    const m = text.match(re);
    if (m && m.index < cutIdx) cutIdx = m.index;
  }
  return text.slice(0, cutIdx);
}

// Filtre une LIGNE individuelle pour exclure les mentions parasites (footer, copyright, décret)
// Utilisé par findDateAfter, findAmountAfter et parseInvoiceLines pour ignorer ces lignes.
const LINE_NOISE_RE = /(d[ée]cret\s+\d{4}|application\s+du\s+d[ée]cret|article\s+L\s*\d+|p[ée]nalit[ée]s?\s+de\s+retard|conditions?\s+g[ée]n[ée]rales|escompte\s+pour\s+r[èe]glement|en\s+cas\s+de\s+retard|sans\s+escompte|art\.?\s*L\s*\d+|RCS\s+\w+\s+\d+|capital\s+(?:social|de)|art\.?\s*293\s*B|recouvrement|indemnit[ée]\s+forfaitaire)/i;

// Cherche un montant après un libellé : sur la même ligne (avant ou après) OU sur les lignes suivantes
function findAmountAfter(text, labels) {
  const lines = text.split(/\r?\n/);
  // Liste de labels "concurrents" : si on cherche TTC et qu'on tombe sur "Total HT", on s'arrête
  const otherTotalLabels = /(?:somme\s*[àa]\s*payer\s*ttc|total\s*ttc|montant\s*ttc|total\s*ht|total\s*tva|total\s*tax|montant\s*ht|sous[\s-]*total\s*ht|net\s*[àa]\s*payer|solde\s*d[uû])/i;

  for (const label of labels) {
    const labelRe = new RegExp('(?:^|\\b)' + label + '(?:\\b|\\s|$|:)', 'i');
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (!labelRe.test(line)) continue;
      if (LINE_NOISE_RE.test(line)) continue;
      // Skip: ligne d'en-tête horizontal de totaux (contient PLUSIEURS labels totaux)
      const lineLc = line.toLowerCase();
      let kwHits = 0;
      if (/total\s*ht/.test(lineLc)) kwHits++;
      if (/total\s*ttc/.test(lineLc)) kwHits++;
      if (/montant\s*tva|tx\s*\/\s*montant/.test(lineLc)) kwHits++;
      if (/net\s*[àa]?\s*payer/.test(lineLc)) kwHits++;
      if (kwHits >= 3) continue; // c'est un en-tête de colonnes, pas un label suivi de valeur

      // 1. Sur la même ligne, après le label
      const parts = line.split(labelRe);
      if (parts.length > 1) {
        const after = parts.slice(1).join(' ');
        const v1 = extractAmountFromLine(after);
        if (v1 != null && v1 > 0) return v1;
      }
      // 2. Sur la même ligne, AVANT le label (cas "894,00 Somme à payer TTC" en ASTBTP)
      if (parts.length > 0 && parts[0]) {
        const before = parts[0];
        const v0 = extractAmountFromLine(before);
        if (v0 != null && v0 > 0) return v0;
      }
      // 3. Sur les 5 lignes suivantes (cas labels en bloc puis valeurs en bloc)
      // MAIS on s'arrête si on tombe sur un AUTRE label de total différent de celui qu'on cherche
      for (let j = i + 1; j < Math.min(i + 6, lines.length); j++) {
        const nextLine = lines[j];
        if (LINE_NOISE_RE.test(nextLine)) continue;
        // Si la ligne contient un autre label de total qui n'est PAS celui qu'on cherche, on arrête
        if (otherTotalLabels.test(nextLine) && !labelRe.test(nextLine)) {
          break;
        }
        const v = extractAmountFromLine(nextLine);
        if (v != null && v > 0) return v;
      }
    }
  }
  return null;
}

// Cas particulier PUM/Richardson : en-tête de totaux sur UNE ligne + valeurs sur LA ligne suivante
// Ex: "TOTAL HT EUR NET HT EUR TX / MONTANT TVA TOTAL TTC NET A PAYER EUR"
//     " 21,00  20,00  4,20  25,20 21,00  0,00"
function parseHorizontalTotals(text) {
  const lines = text.split(/\r?\n/);
  const result = { total: null, vat: null, totalTtc: null };

  for (let i = 0; i < lines.length - 1; i++) {
    const line = lines[i];
    const lc = line.toLowerCase();
    // L'en-tête doit contenir AU MOINS 3 mots-clés totaux
    let hits = 0;
    if (/total\s*ht/.test(lc)) hits++;
    if (/total\s*ttc/.test(lc)) hits++;
    if (/montant\s*tva|tva/.test(lc)) hits++;
    if (/net\s*[àa]?\s*payer|net\s*a\s*payer/.test(lc)) hits++;
    if (hits < 2) continue;
    // Si la ligne contient des chiffres (pas que des labels), on saute
    const numsInHeader = (line.match(/\d+[\.,]?\d*/g) || []).length;
    if (numsInHeader > 1) continue;

    // Trouver les positions des labels-colonnes dans l'en-tête
    const colDefs = [];
    const findCol = (re, type) => {
      const m = line.match(re);
      if (m) colDefs.push({ pos: m.index, len: m[0].length, type });
    };
    findCol(/total\s*ht\s*(?:eur|net)?/i, 'ht');
    findCol(/montant\s*tva|^tx\s*\/\s*montant\s*tva|tx\s*\/\s*montant\s*tva/i, 'vat');
    findCol(/total\s*ttc/i, 'ttc');

    if (colDefs.length < 2) continue;

    // Trier par position
    colDefs.sort((a, b) => a.pos - b.pos);

    // Sur la ligne valeurs, extraire tous les nombres avec leurs positions
    let valuesLine = lines[i + 1];
    if (!valuesLine || !valuesLine.trim()) {
      // sauter ligne vide
      if (i + 2 < lines.length) valuesLine = lines[i + 2];
      else continue;
    }

    const numRe = /(-?\d+(?:[\s\.,]\d{3})*(?:[\.,]\d{1,4})?)/g;
    const nums = [];
    let nm;
    while ((nm = numRe.exec(valuesLine)) !== null) {
      const v = parseFrNumber(nm[1]);
      if (!isNaN(v)) nums.push({ val: v, pos: nm.index });
    }
    if (nums.length < 2) continue;

    // Apparier chaque colonne avec le nombre dont la position est la plus proche
    for (const col of colDefs) {
      let best = null, bestDist = Infinity;
      for (const n of nums) {
        const d = Math.abs(n.pos - col.pos);
        if (d < bestDist) { bestDist = d; best = n; }
      }
      if (best) {
        if (col.type === 'ht' && result.total == null) result.total = Math.abs(best.val);
        if (col.type === 'vat' && result.vat == null) result.vat = Math.abs(best.val);
        if (col.type === 'ttc' && result.totalTtc == null) result.totalTtc = Math.abs(best.val);
      }
    }
    if (result.total != null || result.totalTtc != null) return result;
  }
  return result;
}

// Cas particulier : on a une cascade "Total HT / Total TVA / Total TTC / ..." en colonne
// suivie d'une cascade de valeurs. On apparie label[i] avec value[j] dans l'ordre.
function parseStackedTotals(text) {
  const lines = text.split(/\r?\n/).map(l => l.trim());
  const result = { total: null, vat: null, totalTtc: null };

  // Cherche un bloc de labels totaux consécutifs
  const labelRe = /^(total\s*ht\s*net|total\s*net\s*ht|total\s*ht|port\s*ht|total\s*eco(?:[\.\s].*)?|total\s*tva|total\s*ttc|total\s*ex\s*vat|total\s*tax|sub[\s-]?total|acomptes?|net\s*[àa]\s*payer|solde\s*d[uû]|^total\s*$|amount\s*due)\s*$/i;
  for (let i = 0; i < lines.length - 4; i++) {
    if (!labelRe.test(lines[i])) continue;
    let labels = [];
    let k = i;
    while (k < lines.length && labelRe.test(lines[k])) {
      labels.push(lines[k]);
      k++;
    }
    if (labels.length < 3) { i = k - 1; continue; }
    let j = k;
    while (j < lines.length && lines[j].trim() === '') j++;
    const values = [];
    while (j < lines.length && values.length < labels.length) {
      const t = lines[j].trim();
      if (!t) { j++; continue; }
      const m = t.match(/^(-?[\d\s\.,]+)\s*€?\s*$/);
      if (!m) break;
      const v = parseFrNumber(m[1]);
      if (isNaN(v)) break;
      values.push(v);
      j++;
    }
    if (values.length === labels.length) {
      let htNetIdx = -1, htIdx = -1;
      for (let p = 0; p < labels.length; p++) {
        const lab = labels[p].toLowerCase();
        if (/total\s*(ht\s*net|net\s*ht)/.test(lab)) htNetIdx = p;
        else if (/total\s*ht/.test(lab) && !/eco|port|net/.test(lab)) htIdx = p;
        else if (/total\s*ex\s*vat/.test(lab)) htIdx = p;  // Anglais : Total ex VAT
        if (/total\s*tva/.test(lab) || /total\s*tax/.test(lab)) result.vat = Math.abs(values[p]);
        if (/total\s*ttc/.test(lab) || /^total\s*$/.test(lab) || /amount\s*due/.test(lab)) {
          // "Total" seul = TTC dans ce contexte
          if (result.totalTtc == null) result.totalTtc = Math.abs(values[p]);
        }
      }
      if (htNetIdx >= 0) result.total = Math.abs(values[htNetIdx]);
      else if (htIdx >= 0) result.total = Math.abs(values[htIdx]);
      return result;
    }
  }
  return result;
}

// Extrait le PREMIER nombre cohérent (positif, raisonnable) d'une ligne
function extractAmountFromLine(s) {
  if (!s) return null;
  // Skip lignes qui sont juste des dates ou des codes
  if (/^\s*\d{1,2}\s*[\/\-]\s*\d{1,2}/.test(s.trim())) return null;
  // Skip lignes d'adresse "280 AVENUE...", "13015 MARSEILLE", etc.
  if (/\d+\s+(avenue|rue|bd|boulevard|impasse|all[ée]e|chemin|place|cours|quai|route|av\.?|chem\.?|rte\.?)\b/i.test(s)) return null;
  // Skip codes postaux suivis d'un nom de ville (5 chiffres + lettres)
  if (/^\s*\d{5}\s+[A-ZÀ-Ÿ]/i.test(s.trim())) return null;
  // Skip SIRET (14 chiffres) et SIREN (9 chiffres) avec label
  if (/(?:siret|siren|tva\s*intra|n[°o]?\s*tva|num[ée]ro\s*d[''’]?entreprise|rcs|n[°o]?\s*adh[ée]rent)\s*[:.]/i.test(s)) return null;
  // Skip lignes avec libellé "Facture N°" ou "Invoice no"
  if (/(?:^|\b)(facture|invoice)\s*(?:n[°o]|no\.?|number|#)/i.test(s)) return null;
  // Skip numéros de téléphone (format français)
  if (/\b0[1-9](?:[\s\-\.]?\d{2}){4}\b/.test(s)) return null;
  // Skip IBAN/BIC
  if (/\b(iban|bic|swift)\b/i.test(s)) return null;
  // Skip ligne "Période : ..."
  if (/^\s*p[ée]riode\s*:/i.test(s)) return null;
  // Skip jours de la semaine suivis d'une date (Asplomberie : "Lundi 23/03")
  if (/\b(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche|lun|mar|mer|jeu|ven|sam|dim|monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b/i.test(s)) return null;

  const re = /(-?\d+(?:[\s\.,]\d{3})*(?:[\.,]\d{1,4})?)\s*€?/;
  const m = s.match(re);
  if (m) {
    const v = parseFrNumber(m[1]);
    // Filtre les pourcentages purs (5, 10, 20) si suivis de %
    const idx = m.index + m[0].length;
    const after = s.slice(idx, idx + 3);
    if (/^\s*%/.test(after)) return null;
    // Rejet : si après le nombre on a / ou - puis un autre nombre, c'est une date (ex "23/03")
    if (/^\s*[\/\-]\s*\d/.test(after)) return null;
    if (!isNaN(v) && v >= 0) return v;
  }
  return null;
}

// Cherche le numéro de facture en testant plusieurs patterns + valeur sur ligne suivante
function findInvoiceNumber(text) {
  const lines = text.split(/\r?\n/);
  // Patterns "label: valeur" sur la même ligne
  const inlinePatterns = [
    /facture\s*n[°o]\s*[:#]?\s*([A-Z0-9][A-Z0-9_\-\/\.]{1,25})/i,
    /facture\s*[#]\s*([A-Z0-9][A-Z0-9_\-\/\.]{1,25})/i,
    /n[°o]\s*facture\s*[:#]?\s*([A-Z0-9][A-Z0-9_\-\/\.]{1,25})/i,
    /n[°o]\s*doc(?:ument)?\s*[:#]?\s*([A-Z0-9][A-Z0-9_\-\/\.]{1,25})/i,
    /facture\s*n\s*[:#]?\s*([A-Z0-9][A-Z0-9_\-\/\.]{1,25})/i,
    /facture\s+([A-Z]{1,3}\d{4,12})\b/i,            // "Facture F2600008"
    /facture\s+#?(\d{4}-\d{2}-\d{2})\b/i,            // "Facture #2026-04-11"
    /reference\s*[:#]?\s*([A-Z]{2,5}\d{4,12})/i,
    /facture\s*n[°o]?\.?\s*[:#]?\s*(\d{2,15})/i,    // "Facture n:040426" ou "Facture N° 38476"
    /\bnum[ée]ro\s+([A-Z0-9][A-Z0-9_\-\/\.]{2,25})/i,
  ];
  // Mots qu'on ne veut surtout pas comme numéro
  const blacklist = /^(de|du|le|la|les|sur|page|pour|par|au|chantier|adh[ée]rent|reference|tva|adh|n|client|adresse|date|num|pr[ée]l[èe]vement|virement)$/i;

  for (const re of inlinePatterns) {
    const m = text.match(re);
    if (m && m[1]) {
      const cand = m[1].trim().replace(/[,;.]+$/, '');
      if (cand.length >= 2 && !blacklist.test(cand)) return cand;
    }
  }

  // Patterns "label en bloc, valeur sur ligne suivante"
  // ex: "N° FACTURE\n58", "Reference\nINV484871", "NUMÉRO\n52264100", "Numéro\nM/FA2605436"
  const labelOnLineRe = /^(n[°o]\s*facture|num[ée]ro|reference|facture\s*n[°o]?)\s*$/i;
  for (let i = 0; i < lines.length; i++) {
    if (labelOnLineRe.test(lines[i].trim())) {
      // Chercher la valeur dans les 1-3 lignes suivantes
      for (let j = i + 1; j < Math.min(i + 4, lines.length); j++) {
        const cand = lines[j].trim().split(/\s+/)[0];
        if (cand && cand.length >= 2 && /^[A-Z0-9][A-Z0-9_\-\/\.]+$/i.test(cand) && !blacklist.test(cand)) {
          return cand;
        }
      }
    }
  }

  return '';
}

// Détecte si la facture est en TVA non applicable
function isVatExempt(text) {
  return /tva\s*non\s*applicable|art(?:icle)?\.?\s*293\s*B|exempt(?:é|ee)\s*de\s*tva/i.test(text);
}

function parseInvoice(rawText) {
  // Travailler sur une version tronquée pour les méta (sans CGV/footer)
  const text = trimFooter(rawText);

  const result = {
    supplier: '', number: '', date: '', dueDate: '',
    total: null, vat: null, totalTtc: null,
    paymentMode: 'virement', lines: [],
  };

  // ============== 1. Fournisseur ==============
  for (const p of SUPPLIER_PATTERNS) {
    if (p.re.test(text)) { result.supplier = p.name; break; }
  }
  if (!result.supplier) {
    // fournisseurs déjà connus
    for (const sid in STATE.suppliers) {
      const s = STATE.suppliers[sid];
      if (!s.name) continue;
      const re = new RegExp('\\b' + s.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b', 'i');
      if (re.test(text)) { result.supplier = s.name; break; }
    }
  }
  if (!result.supplier) result.supplier = autoDetectSupplier(text);

  // ============== 2. N° de facture ==============
  result.number = findInvoiceNumber(text);

  // ============== 3. Date de facture ==============
  // Patterns prioritaires : labels EXPLICITES de date facture
  result.date = findDateAfter(text, [
    'date\\s*(?:de\\s*)?(?:la\\s*)?facture',
    'date\\s*(?:de\\s*)?facturation',
    "date\\s*d[\'’]?émission",
    "date\\s*d[\'’]?emission",
    'invoice\\s*date',
  ]);  // "Facture N° XXX du <date>" ou "Facture du <date>"
  if (!result.date) {
    let m = text.match(/facture\s*n[°o]?\.?\s*[:#]?\s*[A-Z0-9_\-\.\/]{1,25}\s*du\s+([\dA-Za-zéûôâèà\s\/\-\.]{6,30})/i);
    if (m) result.date = tryExtractDate(m[1]);
  }
  if (!result.date) {
    let m = text.match(/facture\s+du\s+([\dA-Za-zéûôâèà\s\/\-\.]{6,30})/i);
    if (m) result.date = tryExtractDate(m[1]);
  }
  // "DU 22 avril 2026" sur une ligne propre
  if (!result.date) {
    const lines = text.split(/\r?\n/);
    for (const l of lines) {
      const m = l.match(/^\s*du\s+(\d{1,2}\s+(?:janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre)\.?\s+\d{2,4})/i);
      if (m) { result.date = tryExtractDate(m[1]); if (result.date) break; }
    }
  }
  // "Le DD/MM/YYYY" en début de ligne
  if (!result.date) {
    const lines = text.split(/\r?\n/);
    for (const l of lines.slice(0, 30)) {
      const m = l.match(/^\s*Le\s+(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})\b/i);
      if (m) { result.date = tryExtractDate(m[1]); break; }
    }
  }
  // "DATE" seul comme libellé de colonne, valeur sur ligne suivante
  if (!result.date) {
    result.date = findDateAfter(text, ['^date\\s*$']);
  }
  // Dernier recours : la PREMIÈRE date numérique trouvée dans les 30 premières lignes
  // MAIS en EXCLUANT les contextes "estim", "livraison", "intervention", "échéance", "exigible"
  if (!result.date) {
    const lines = text.split(/\r?\n/).slice(0, 40);
    for (const l of lines) {
      if (/estim|livraison|intervention|[ée]ch[ée]ance|exigible|payer|payment\s*terms/i.test(l)) continue;
      if (LINE_NOISE_RE.test(l)) continue;
      const m = l.match(/\b(\d{1,2}\s*[\/\-\.]\s*\d{1,2}\s*[\/\-\.]\s*\d{2,4})\b/);
      if (m) { result.date = tryParseDate(m[1]); if (result.date) break; }
    }
  }
  if (!result.date) {
    const lines = text.split(/\r?\n/).slice(0, 40);
    for (const l of lines) {
      if (/estim|livraison|intervention|[ée]ch[ée]ance|exigible/i.test(l)) continue;
      if (LINE_NOISE_RE.test(l)) continue;
      const m = l.match(/\b(\d{1,2}\s+(?:janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|septembre|octobre|novembre|décembre|decembre|january|february|march|april|may|june|july|august|september|october|november|december)\.?\s+\d{2,4})\b/i);
      if (m) { result.date = tryParseDate(m[1]); if (result.date) break; }
    }
  }

  // ============== 4. Date d'échéance ==============
  result.dueDate = findDateAfter(text, [
    "date\\s*d[\'’]?\\s*[ée]ch[ée]ance",
    "[ée]ch[ée]ance",
    "exigible\\s*(?:le)?",
    "[àa]\\s*payer\\s*(?:avant|le)",
    "payable\\s*(?:le|au)",
  ]);
  // "Reste à payer 129,10 EUR au 02/05/2026" → cherche "au DD/MM/YYYY"
  if (!result.dueDate) {
    const m = text.match(/reste\s*[àa]\s*payer.{0,40}au\s+(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})/i);
    if (m) result.dueDate = tryParseDate(m[1]);
  }
  // Échéance relative
  if (!result.dueDate && result.date) {
    result.dueDate = applyRelativeDue(result.date, text);
  }

  // Fallback numéro : si on a une date mais pas de numéro, chercher dans les 3 lignes
  // précédant la date un nombre court (5-15 caractères, alphanumériques) qui ressemble à un numéro
  if (!result.number && result.date) {
    const lines = text.split(/\r?\n/);
    const dateRe = /(\d{1,2}\s*[\/\-\.]\s*\d{1,2}\s*[\/\-\.]\s*\d{2,4})/;
    for (let i = 0; i < lines.length; i++) {
      const m = lines[i].match(dateRe);
      if (m) {
        const d = tryParseDate(m[1]);
        if (d === result.date) {
          // Cherche dans les 3 lignes précédentes un numéro PUREMENT numérique ou avec lettres+chiffres
          for (let j = i - 1; j >= Math.max(0, i - 4); j--) {
            const t = lines[j].trim();
            // Doit être : seulement chiffres (5-15) OU lettres-suivies-de-chiffres
            if (/^\d{5,15}$/.test(t) || /^[A-Z]{1,4}\d{4,12}$/i.test(t) || /^\d{1,4}[\.\-\/]\d{2,8}([\.\-\/]\d{2,8})?$/.test(t)) {
              // Refuser si ressemble à une date pure
              if (/^\d{1,2}[\.\-\/]\d{1,2}[\.\-\/]\d{2,4}$/.test(t)) continue;
              // Refuser numéros trop courts
              if (t.length < 4) continue;
              result.number = t;
              break;
            }
          }
          if (result.number) break;
        }
      }
    }
  }

  // ============== 5. Mode de règlement ==============
  if (/virement\s*instantan[ée]/i.test(text)) result.paymentMode = 'virement';
  else if (/virement/i.test(text)) result.paymentMode = 'virement';
  else if (/pr[ée]l[èe]vement|automatique\s*pr[ée]lev/i.test(text)) result.paymentMode = 'prelevement';
  else if (/ch[èe]que/i.test(text)) result.paymentMode = 'cheque';
  else if (/carte\s*bancaire|\bcb\b|paid\s*by\s*card/i.test(text)) result.paymentMode = 'cb';
  else if (/esp[èe]ces?\b/i.test(text)) result.paymentMode = 'especes';
  else if (/\blcr\b|traite/i.test(text)) result.paymentMode = 'lcr';

  // ============== 6-7-8. Totaux : tentative parseStackedTotals d'abord (cas Aredis) ==============
  const stacked = parseStackedTotals(text);
  if (stacked.total != null) result.total = stacked.total;
  if (stacked.vat != null) result.vat = stacked.vat;
  if (stacked.totalTtc != null) result.totalTtc = stacked.totalTtc;

  // ============== 6. Total HT ==============
  if (result.total == null) {
    result.total = findAmountAfter(text, [
    'total\\s*ht\\s*net',
    'total\\s*net\\s*ht',
    'total\\s*ex\\s*vat',
    'sous[\\s-]*total\\s*h\\.?t\\.?',
    'total\\s*de\\s*la\\s*facture\\s*ht',
    'total\\s*hors\\s*taxes?',
    'total\\s*h\\.?t\\.?',
    'montant\\s*total\\s*ht',
    'montant\\s*ht',
    'total\\s*forfait\\s*ht',
    'forfait\\s*total\\s*ht',
    'total\\s*ht',
    ]);
  }

  // ============== 7. TVA ==============
  if (isVatExempt(text) && result.vat == null) {
    result.vat = 0;
  } else if (result.vat == null) {
    result.vat = findAmountAfter(text, [
      'total\\s*tva',
      'montant\\s*tva',
      'total\\s*tax(?!es)',
      '^\\s*tva\\s*$',
    ]);
    if (result.vat != null && (result.vat < 0 || result.vat > 100000)) result.vat = null;
  }

  // ============== 8. Total TTC ==============
  if (result.totalTtc == null) {
    result.totalTtc = findAmountAfter(text, [
    'somme\\s*[àa]\\s*payer\\s*ttc',
    'total\\s*de\\s*la\\s*facture\\s*ttc',
    'total\\s*ttc',
    'montant\\s*ttc',
    'total\\s*toutes\\s*taxes\\s*comprises',
    'net\\s*[àa]\\s*payer\\s*ttc',
    'net\\s*[àa]\\s*payer',
    'solde\\s*d[uû]',
    'amount\\s*due',
    '^total\\s*$',  // "Total" seul sur ligne
    '^total(?!\\s+(?:ht|hors|de\\s+la\\s+facture\\s+ht|net\\s+ht|tva|tax|ex))\\b',  // "TOTAL ..." mais pas "TOTAL HT", "TOTAL DE LA FACTURE HT", etc.
    ]);
  }
  // Fallback PUM : "Acompte perçu n° XXX du DD/MM/YY :   25,20"
  if (result.totalTtc == null) {
    const m = text.match(/acompte\s*per[çc]u[^:]*:\s*(\d+[\.,]\d+)/i);
    if (m) {
      const v = parseFrNumber(m[1]);
      if (!isNaN(v) && v > 0) result.totalTtc = v;
    }
  }
  // Fallback VAT spécifique : "Dont TVA perçue : 4,20" (PUM)
  if (result.vat == null || result.vat === 0) {
    const m = text.match(/dont\s+tva\s+per[çc]ue\s*:?\s*(\d+[\.,]\d+)/i);
    if (m) {
      const v = parseFrNumber(m[1]);
      if (!isNaN(v) && v > 0) result.vat = v;
    }
  }
  // Fallback Bruneau : "20,00 % 107,58 21,52" + ligne suivante "129,10"
  // → taux=20% / base HT=107,58 / TVA=21,52, puis TTC sur ligne suivante
  // Note : Bruneau a parfois un HT principal (105) + des frais (2.58) qui forment
  // une "base soumise à TVA" différente. Le vrai TTC est donné explicitement.
  {
    const lines = text.split(/\r?\n/);
    for (let i = 0; i < lines.length - 1; i++) {
      const line = lines[i];
      // Pattern "20,00 % 107,58 21,52" — un taux suivi de 2 nombres
      const m = line.match(/^\s*(\d+[\.,]\d{1,2})\s*%\s+(\d+(?:[\s\.,]\d{3})*[\.,]\d{1,2})\s+(\d+(?:[\s\.,]\d{3})*[\.,]\d{1,2})\s*$/);
      if (m) {
        const tauxV = parseFrNumber(m[1]);
        const baseV = parseFrNumber(m[2]);
        const vatV = parseFrNumber(m[3]);
        // Vérifier cohérence : base * taux/100 ≈ vat
        if (Math.abs(baseV * tauxV / 100 - vatV) < 0.05 && baseV > 0 && vatV > 0) {
          if (result.vat == null) result.vat = vatV;
          // Le TTC est sur la ligne suivante (un seul nombre)
          for (let j = i + 1; j < Math.min(i + 4, lines.length); j++) {
            const tn = lines[j].trim();
            if (!tn) continue;
            const m2 = tn.match(/^\s*(\d+(?:[\s\.,]\d{3})*[\.,]\d{1,2})\s*€?\s*$/);
            if (m2) {
              const ttcV = parseFrNumber(m2[1]);
              if (Math.abs(baseV + vatV - ttcV) < 0.05) {
                // On force le TTC trouvé (priorité à cette détection sur la cohérence)
                result.totalTtc = ttcV;
                break;
              }
            }
          }
          break;
        }
      }
    }
  }

  // 9. Cohérence HT / TVA / TTC
  if (result.total != null && result.totalTtc == null) {
    if (result.vat != null) result.totalTtc = +(result.total + result.vat).toFixed(2);
    else result.totalTtc = +(result.total * 1.20).toFixed(2);
  } else if (result.total == null && result.totalTtc != null) {
    if (result.vat != null && result.vat > 0) result.total = +(result.totalTtc - result.vat).toFixed(2);
    else if (result.vat === 0) result.total = result.totalTtc;
    else result.total = +(result.totalTtc / 1.20).toFixed(2);
  }
  if (result.total != null && result.totalTtc != null && result.vat == null) {
    result.vat = +(result.totalTtc - result.total).toFixed(2);
  }
  // Sanity check : si TVA non applicable mais HT < TTC, forcer HT = TTC
  // (cas Asplomberie : "Total HT" est un en-tête de colonne mal capté, le vrai HT = TTC)
  if (result.vat === 0 && result.totalTtc != null && result.total != null
      && result.total < result.totalTtc - 0.01) {
    result.total = result.totalTtc;
  }
  // Sanity check : si HT et TTC identiques mais TVA détectée > 0, on corrige TTC
  // (cas PUM : HT=21 et "TOTAL TTC" a aussi capté 21 par erreur, mais TVA=4.20 est correcte)
  if (result.total != null && result.totalTtc != null && result.vat != null && result.vat > 0
      && Math.abs(result.total - result.totalTtc) < 0.01) {
    result.totalTtc = +(result.total + result.vat).toFixed(2);
  }
  // Sanity check : si HT et TTC identiques ET TVA non détectée (ou null), on force VAT=0
  if (result.total != null && result.totalTtc != null && Math.abs(result.total - result.totalTtc) < 0.01
      && (result.vat == null || result.vat === 0)) {
    result.vat = 0;
  }
  // Sanity : TTC ne peut pas être < HT
  if (result.total != null && result.totalTtc != null && result.totalTtc < result.total - 0.01) {
    [result.total, result.totalTtc] = [result.totalTtc, result.total];
    result.vat = +(result.totalTtc - result.total).toFixed(2);
  }
  // Sanity : si la TVA est PLUS GRANDE que le HT (cas ASTBTP : "TVA"→745, en réalité HT)
  // ET que HT+VAT = TTC (mathématiquement OK), on inverse HT et VAT
  if (result.total != null && result.vat != null && result.totalTtc != null) {
    if (result.vat > result.total && Math.abs(result.total + result.vat - result.totalTtc) < 0.05) {
      // Vérifier que le swap fait sens : nouvelle TVA doit être 5%, 10%, 20% du nouveau HT
      const newHt = result.vat;
      const newVat = result.total;
      const ratio = newVat / newHt;
      if (ratio > 0.04 && ratio < 0.25) {
        result.total = newHt;
        result.vat = newVat;
      }
    }
  }

  // ============== 10. Lignes article ==============
  result.lines = parseInvoiceLines(text, result);

  return result;
}

// Lignes à ignorer (entêtes, totaux, sous-lignes parasites)
/**
 * Préprocesseur : reconstruit les lignes "à colonnes éclatées" comme dans Asplomberie.
 * Deux cas gérés :
 *  A) En-tête de tableau sur UNE ligne avec plusieurs mots-clés → on regroupe les
 *     lignes suivantes par paquets de N
 *  B) En-tête éclaté EN COLONNE (chaque mot-clé sur sa propre ligne) → on détecte un
 *     bloc consécutif de mots-clés colonnes, on en déduit N, et on regroupe.
 */
function reglueColumnLines(text) {
  const lines = text.split(/\r?\n/);
  const out = [];
  let i = 0;

  // Mots-clés d'en-têtes de colonnes (français + anglais, mono ou multi-mots)
  const isHeaderToken = (s) => {
    const t = s.toLowerCase().trim().replace(/[^a-z0-9àéèêûôî\s]/g, '').trim();
    if (!t) return false;
    return /^(description|type|qt[ée]|quantit[ée]|quantity|prix(\s+unitaire(\s+ht)?)?|price|selling\s*price?|montant|amount|gross(\s+amount)?|total(\s+ht|\s+ttc)?|unit[ée]?|p\.?u\.?(\s+ht)?|tva|vat(\s+\%)?|d[ée]signation|hors\s+taxe|t\.?t\.?c\.?|net|nº|n°|n\b|code|reference|ref|garantie|incidence)$/.test(t);
  };

  while (i < lines.length) {
    const line = lines[i].trim();

    // ----- CAS A : en-tête sur une seule ligne -----
    const headerKeywords = ['description', 'type', 'qté', 'qte', 'quantité', 'quantite', 'quantity', 'prix', 'price', 'montant', 'amount', 'total', 'unité', 'unite', 'unit', 'p.u', 'pu', 'tva', 'designation', 'désignation'];
    const lineLc = line.toLowerCase();
    const matchedKw = headerKeywords.filter(k => lineLc.includes(k));

    if (matchedKw.length >= 3 && line.split(/\s+/).length <= 12) {
      out.push(line);
      i++;
      let consecutiveShort = 0, probeIdx = i;
      while (probeIdx < Math.min(i + 8, lines.length) && lines[probeIdx].trim().length > 0) {
        if (lines[probeIdx].trim().length < 30) consecutiveShort++;
        probeIdx++;
      }
      if (consecutiveShort >= 4) {
        const remaining = collectTableLines(lines, i);
        const merged = regroupByGuessedN(remaining, matchedKw.length);
        out.push(...merged);
        i += remaining.length;
        continue;
      }
    }

    // ----- CAS B : en-tête éclaté (mots-clés en colonne, chacun sur sa ligne) -----
    if (isHeaderToken(line)) {
      let headerCount = 1;
      let probeIdx = i + 1;
      while (probeIdx < lines.length && isHeaderToken(lines[probeIdx])) {
        headerCount++;
        probeIdx++;
      }
      if (headerCount >= 3) {
        // On a détecté un en-tête éclaté de N colonnes
        const N = headerCount;
        // Garde les en-têtes en sortie (concaténés)
        out.push(lines.slice(i, i + N).map(s => s.trim()).join(' | '));
        i = probeIdx;
        // Collecter les lignes du tableau
        const remaining = collectTableLines(lines, i);
        const merged = regroupByGuessedN(remaining, N);
        out.push(...merged);
        i += remaining.length;
        continue;
      }
    }

    out.push(line);
    i++;
  }
  return out.join('\n');
}

// Collecte les lignes successives d'un tableau jusqu'à un séparateur (total/ligne vide/footer)
function collectTableLines(lines, startIdx) {
  const remaining = [];
  for (let k = startIdx; k < lines.length; k++) {
    const t = lines[k].trim();
    if (!t) {
      // Ligne vide : on arrête seulement si on a déjà collecté quelque chose
      if (remaining.length > 0 && remaining.length % 4 < 2) break;
      continue;
    }
    if (/^(total|sous-total|tva|h\.?t\.?\s|t\.?t\.?c\.?|net\s|montant|conditions|page|taxe\s|merci|escompte|p[ée]nalit|sub[\s-]?total|amount\s+due|total\s+ex\s+vat|payment\s+terms)/i.test(t)) break;
    remaining.push(t);
    if (remaining.length > 200) break;
  }
  return remaining;
}

// Regroupe les lignes par paquets de N (essaie aussi des N voisins pour trouver le meilleur)
function regroupByGuessedN(remaining, hintN) {
  let bestN = hintN;
  let bestScore = -1;
  // Essayer N = hintN, puis 5, 4, 3, 6
  const candidates = [hintN, 5, 4, 3, 6, 2].filter((n, idx, arr) => arr.indexOf(n) === idx && n >= 2 && n <= 8);
  for (const N of candidates) {
    if (remaining.length < N) continue;
    let score = 0;
    let validBlocks = 0;
    for (let k = 0; k + N <= remaining.length; k += N) {
      const block = remaining.slice(k, k + N).join(' ');
      const nums = (block.match(/\d+[\.,]?\d*/g) || []).length;
      // Une ligne d'article réelle a 2-4 nombres (qté, pu, total, +éventuel %TVA)
      if (nums >= 2 && nums <= 6) score++;
      validBlocks++;
    }
    // Préférer N qui maximise score, puis qui colle au hintN
    const fit = score / Math.max(1, validBlocks);
    if (fit > bestScore || (fit === bestScore && N === hintN)) {
      bestScore = fit;
      bestN = N;
    }
  }
  const result = [];
  for (let k = 0; k + bestN <= remaining.length; k += bestN) {
    const merged = remaining.slice(k, k + bestN).join(' ').replace(/\s+/g, ' ').trim();
    result.push(merged);
  }
  const leftover = remaining.length % bestN;
  if (leftover > 0) {
    const merged = remaining.slice(-leftover).join(' ').replace(/\s+/g, ' ').trim();
    if (merged && (merged.match(/\d/g) || []).length >= 2) result.push(merged);
  }
  return result;
}

const SKIP_LINE_RE = /^(total|sous[\s-]*total|tva|h\.?t\.?\s*$|t\.?t\.?c\.?|montant|remise|escompte|page\b|date\b|n[°o]\s|client|adresse|tel\b|fax\b|email|siret|tva\s*intra|conditions|mode\s*de|chantier|commande|pris\s*par|email|adresse|si[èe]ge|domicili|iban\b|bic\b|rib\b|paiement|num[ée]ro\b|code\s*client|coordonn[ée]es|banque\b|merci\b|en\s*cas|p[ée]nalit|conditions|garantie|date\s*estim|date\s*intervention|net\s*[aà]\s*payer|solde\s*d[uû]|acompte|reprise\s*sur\s*acompte|dont\s*tva|escompte|sommes?\s*d[ée]j|p[ée]riode\s*:|exigible|n[°o]\s*adh|expiration|expedition|exp[ée]dition|réf[ée]rence\s*comptable|p[ée]riode|d[ée]livré|origine|votre\s*ref|votre\s*commande|total\s*ex|payment\s*terms|account|reference|description\s*$|d[ée]signation\s*$|qte|qt[ée]\s*$|prix|montant\s*$|unit[ée]?\s*$)/i;

const SKIP_SUB_LINE_RE = /^(dont\s+|de\s+|au\s+|le\s+|sur\s+|pour\s+|en\s+|à\s+|par\s+)/i;
const ECO_LINE_RE = /eco[\s\-]*(?:part|contrib|cotisat|emball|deee|mobilier)/i;
const FOOTER_RE = /(escompte|p[ée]nalit|d[ée]cret|article|merci|veuillez|nous\s+vous|paiement|virement|i?ban|bic|swift|signature|page\s+\d+\s*\/\s*\d+|sur\s+\d+|dispens[ée]|domicili|conditions?\s+g[ée]n)/i;

/**
 * Parse les lignes article d'un texte de facture.
 * Heuristique : chaque ligne se termine généralement par 2-3 nombres (qté, PU, total ligne)
 * et le total = qté × PU (à 6% près).
 */
function parseInvoiceLines(text, parsedInvoice) {
  const lines = [];
  // Préprocessing : reconstruire les lignes éclatées en colonnes
  const preprocessed = reglueColumnLines(text);
  const rawLines = preprocessed.split(/\r?\n/).map(l => l.trim()).filter(Boolean);

  // Garde les totaux pour ne pas confondre la ligne "Total HT 94,99"
  const knownTotals = new Set();
  if (parsedInvoice) {
    if (parsedInvoice.total != null) knownTotals.add(Math.round(parsedInvoice.total * 100));
    if (parsedInvoice.totalTtc != null) knownTotals.add(Math.round(parsedInvoice.totalTtc * 100));
    if (parsedInvoice.vat != null) knownTotals.add(Math.round(parsedInvoice.vat * 100));
  }

  for (let i = 0; i < rawLines.length; i++) {
    const raw = rawLines[i];

    // ----- Filtres préliminaires -----
    if (raw.length < 6) continue;
    if (SKIP_LINE_RE.test(raw)) continue;
    if (FOOTER_RE.test(raw)) continue;
    if (ECO_LINE_RE.test(raw)) continue;
    if (SKIP_SUB_LINE_RE.test(raw)) continue; // sous-lignes "dont Eco-Part..."
    // Lignes types tableau de TVA : "20,00 94,99 19,00" sans texte alpha
    const alphaCount = (raw.match(/[a-zA-ZÀ-ÿ]/g) || []).length;
    // Si très peu de lettres, on accepte uniquement si la ligne PRÉCÉDENTE a une description
    // (cas Bruneau : "FI1 8304-915 EPSON..." puis "1 105,00 105,00 20,00")
    let descFromPrev = '';
    if (alphaCount < 4) {
      // Chercher description sur 1-2 lignes précédentes (pas un en-tête de tableau)
      for (let p = i - 1; p >= Math.max(0, i - 3); p--) {
        const prev = rawLines[p];
        if (!prev || prev.length < 4) continue;
        if (SKIP_LINE_RE.test(prev) || FOOTER_RE.test(prev) || ECO_LINE_RE.test(prev)) continue;
        const prevAlpha = (prev.match(/[a-zA-ZÀ-ÿ]/g) || []).length;
        if (prevAlpha >= 4) {
          // Vérifier que cette ligne précédente n'a pas déjà été utilisée comme ligne article
          // (heuristique : peu de nombres en fin)
          const prevNums = (prev.match(/\d+[\.,]\d+/g) || []).length;
          if (prevNums <= 1) {
            descFromPrev = prev;
            break;
          }
        }
      }
      if (!descFromPrev) continue; // pas trouvé de description, on skip
    }
    // Filtres URL et codes longs
    if (/https?:\/\/|www\.|@/.test(raw)) continue;
    // Nettoyage : enlever "...." (points de suite Richardson) et remplacer par espace
    const cleaned = raw.replace(/\.{3,}/g, ' ').replace(/\s+/g, ' ').trim();

    // ----- Extraction des nombres -----
    // Pattern : nombre potentiellement préfixé €. On accepte . et , comme séparateurs
    // de milliers MAIS PAS l'espace (trop ambigu : "1 293,00" peut être "1" qty + "293,00" total)
    const numRe = /(-?\d+(?:[\.,]\d{3})*(?:[\.,]\d{1,4})?)\s*€?/g;
    const allNums = [];
    let m;
    while ((m = numRe.exec(cleaned)) !== null) {
      const v = parseFrNumber(m[1]);
      if (!isNaN(v)) allNums.push({ str: m[1], val: v, idx: m.index, len: m[0].length });
    }
    if (allNums.length < 2) continue;

    // ----- Détecter total + PU + qty (en partant de la fin) -----
    // Heuristique : si le dernier nombre est <= 25 et qu'il y a 4+ nombres dont l'avant-dernier
    // est >= 1, le dernier est probablement un % TVA et le total est l'avant-dernier
    let nums = allNums.slice(); // copie
    if (nums.length >= 4) {
      const last = nums[nums.length - 1];
      // %TVA typique : 0, 5, 5.5, 10, 20 (et parfois autres taux <=25)
      // Et le nombre avant est un montant > 1 (pas un autre %)
      const beforeLast = nums[nums.length - 2];
      if (last.val >= 0 && last.val <= 25 && beforeLast.val > 1) {
        // Vérifier si l'avant-dernier serait cohérent comme total ligne
        // Par exemple : "1 105,00 105,00 20,00" → last=20 (%TVA), beforeLast=105 (total)
        nums = nums.slice(0, -1);
      }
    }

    let qty = null, pu = null, totalLine = null, qtyN = null, puN = null;
    const lastN = nums[nums.length - 1];

    // Note : on ne skippe PAS si lastN matche un total connu — un article dont le total
    // est égal au total HT global est parfaitement valide (Bruneau : 1 article à 105€,
    // total global = 105€ aussi). On vérifie juste que ce n'est pas la ligne "Total HT 105".
    // Détection : si la ligne contient un mot-clé "total" ET un seul nombre, on skip.
    if (knownTotals.has(Math.round(lastN.val * 100))) {
      // Skip seulement si la ligne ressemble à une ligne de TOTAL global, pas à un article
      const looksLikeTotalLine = /^(?:total|sous[\s-]*total|montant|net\s+[àa]\s+payer|tva|hors|ttc|ht|amount|sub[\s-]?total)\b/i.test(cleaned);
      if (looksLikeTotalLine) continue;
      // Sinon on continue (ça peut être une ligne d'article dont le total = total global)
    }

    totalLine = lastN.val;
    if (totalLine <= 0 || totalLine > 100000) continue;

    // Cherche pu et qty : on teste des paires (pu, qty) parmi les derniers nombres
    let bestMatch = null;
    for (let pi = nums.length - 2; pi >= Math.max(0, nums.length - 5); pi--) {
      const candPu = nums[pi];
      if (candPu.val <= 0 || candPu.val > 50000) continue;

      // Cas A : pas de qty (qty=1, pu=total)
      if (Math.abs(candPu.val - totalLine) < 0.02 && totalLine < 50000) {
        // Score privilégiant les "vrais" prix unitaires (pas 1 ni des nombres ronds suspects)
        let scoreA = 0.9;
        if (candPu.val > 1.5) scoreA += 0.4; // probable PU réel
        if (!bestMatch || scoreA > bestMatch.score) {
          bestMatch = { qty: 1, pu: candPu.val, qtyN: null, puN: candPu, score: scoreA };
        }
      }
      // Cas B : qty est un nombre avant le pu
      for (let qi = pi - 1; qi >= Math.max(0, pi - 3); qi--) {
        const candQ = nums[qi];
        if (candQ.val <= 0 || candQ.val > 10000) continue;
        const expected = candQ.val * candPu.val;
        const diff = Math.abs(expected - totalLine);
        const rel = diff / totalLine;
        if (rel < 0.02 || diff < 0.02) {
          // Score : préférer les combinaisons où pu > 1 (un PU unitaire est rarement = 1)
          let score = 1 - rel;
          // Bonus si pu > 1 (PU plus probable que la quantité)
          if (candPu.val > 1.5) score += 0.5;
          // Bonus si qty est un entier "raisonnable"
          if (Number.isInteger(candQ.val) && candQ.val <= 100) score += 0.3;
          // Malus si qty a beaucoup de décimales (probable que ce soit un PU déguisé)
          if (!Number.isInteger(candQ.val) && candQ.val > 10) score -= 0.4;
          // Malus si pu == 1 et qty ressemble à un montant (>10)
          if (candPu.val === 1 && candQ.val > 10) score -= 0.5;
          if (!bestMatch || score > bestMatch.score) {
            bestMatch = { qty: candQ.val, pu: candPu.val, qtyN: candQ, puN: candPu, score };
          }
        }
      }
    }

    if (!bestMatch) continue;
    qty = bestMatch.qty;
    pu = bestMatch.pu;
    qtyN = bestMatch.qtyN;
    puN = bestMatch.puN;

    // ----- Texte à gauche = code + désignation -----
    const cutIdx = qtyN ? qtyN.idx : puN.idx;
    let leftPart = cleaned.slice(0, cutIdx).trim();
    // Si la qty est en début de ligne (pas de texte avant), la description est ENTRE qty et pu
    // (cas BigChange : "1.000 DEV/1315 - JobWatch Software ... 59.95 0.00 0.00 59.95")
    if ((!leftPart || leftPart.length < 3) && qtyN && puN) {
      const middlePart = cleaned.slice(qtyN.idx + qtyN.len, puN.idx).trim();
      if (middlePart && middlePart.length >= 3) {
        leftPart = middlePart;
      }
    }
    // Si pas assez de texte à gauche mais qu'on a une description sur la ligne précédente, l'utiliser
    if ((!leftPart || leftPart.length < 3) && descFromPrev) {
      leftPart = descFromPrev;
    } else if (!leftPart || leftPart.length < 3) {
      continue;
    }

    // Détecter unité (U, ML, KG, etc.) en fin de leftPart
    let unit = 'U';
    const unitMatch = leftPart.match(/\s+(U|UN|ML|M[éE]TRE|M2|M²|M³|M3|KG|G|L|BO[ÎI]TE|LOT|PI[èE]CE|JOURS|JOUR|HEURE|H|FORFAIT|EN[Ss])\s*$/i);
    if (unitMatch) {
      unit = unitMatch[1].toUpperCase();
      leftPart = leftPart.slice(0, unitMatch.index).trim();
      if (unit === 'METRE' || unit === 'MÉTRE' || unit === 'MÈTRE') unit = 'ML';
    }

    // Code = premier token alphanumérique si formaté comme un code (lettres + chiffres + tirets)
    let code = '';
    let desig = leftPart;
    const codeMatch = leftPart.match(/^([A-Z][A-Z0-9_\-\.\/]{2,18}|\d{4,12}|[A-Z]{1,4}\d{2,12})\s+(.+)$/i);
    if (codeMatch) {
      // Vérifie que c'est un code et pas le début d'une phrase
      const tok = codeMatch[1];
      if (tok.length >= 3 && /[\d\-\/]/.test(tok)) {
        code = tok;
        desig = codeMatch[2];
      }
    }

    desig = desig.replace(/^[\s\-:]+|[\s\-:]+$/g, '').replace(/\s+/g, ' ').trim();
    // Si la désignation est vide ou trop courte ET qu'on a trouvé une description sur la ligne précédente, l'utiliser
    if ((desig.length < 2 || /^\d+$/.test(desig)) && descFromPrev) {
      // Extraire code de la prev si présent
      const prevCodeMatch = descFromPrev.match(/^([A-Z][A-Z0-9_\-\.\/]{2,18}|\d{5,12}|[A-Z]{1,4}\d{2,12})\s+(.+)$/i);
      if (prevCodeMatch && /[\d\-\/]/.test(prevCodeMatch[1])) {
        if (!code) code = prevCodeMatch[1];
        desig = prevCodeMatch[2];
      } else {
        desig = descFromPrev;
      }
      desig = desig.replace(/^[\s\-:]+|[\s\-:]+$/g, '').replace(/\s+/g, ' ').trim();
    }
    if (desig.length < 2) continue;
    if (desig.length > 100) desig = desig.slice(0, 100);

    // Filtres anti-faux-positifs
    if (/^(date|n[°o]|page|tva|total|montant|sous|net|reste|escompte|reference|libell|code\b)/i.test(desig)) continue;

    lines.push({ code, designation: desig, qty, unit, pu, total: totalLine });
  }

  return lines;
}

// ============================================================
// MATCHING & ÉCARTS
// ============================================================

function findArticleMatch(line, supplierName) {
  // 1. Match par code (exact, insensible à la casse)
  if (line.code) {
    for (const id in STATE.articles) {
      const a = STATE.articles[id];
      if (!a.code) continue;
      if (a.code.toLowerCase() === line.code.toLowerCase()) {
        // priorité au même fournisseur
        if (!supplierName || !a.supplier || normalize(a.supplier) === normalize(supplierName)) {
          return { article: a, id, score: 1.0 };
        }
      }
    }
    // match code sans fournisseur
    for (const id in STATE.articles) {
      const a = STATE.articles[id];
      if (a.code && a.code.toLowerCase() === line.code.toLowerCase()) {
        return { article: a, id, score: 0.9 };
      }
    }
  }

  // 2. Match par désignation (similarité simple — tokens communs)
  if (line.designation) {
    const tokens = normalize(line.designation).split(' ').filter(t => t.length > 2);
    if (tokens.length === 0) return null;
    let best = null;
    for (const id in STATE.articles) {
      const a = STATE.articles[id];
      if (!a.designation) continue;
      if (supplierName && a.supplier && normalize(a.supplier) !== normalize(supplierName)) continue;
      const aTokens = normalize(a.designation).split(' ').filter(t => t.length > 2);
      if (aTokens.length === 0) continue;
      const inter = tokens.filter(t => aTokens.includes(t));
      const score = inter.length / Math.max(tokens.length, aTokens.length);
      if (score > 0.55 && (!best || score > best.score)) {
        best = { article: a, id, score };
      }
    }
    return best;
  }
  return null;
}

function computeEcart(line, article) {
  if (!article || !article.price) return { pct: null, level: 'noref', overcost: 0 };
  const ref = article.price;
  const pct = ((line.pu - ref) / ref) * 100;
  const tol = (article.tolerance != null && article.tolerance !== '') ? parseFloat(article.tolerance) : null;
  const warnT = tol != null ? tol : STATE.config.threshWarn;
  const dangerT = STATE.config.threshDanger;
  let level = 'ok';
  if (pct > dangerT) level = 'danger';
  else if (pct > warnT) level = 'warn';
  const overcost = (line.pu - ref) * line.qty;
  return { pct, level, overcost };
}

// ============================================================
// PAIEMENTS, STATUTS, BALANCE ÂGÉE
// ============================================================

const STATUS_LABELS = {
  to_check: 'À vérifier',
  validated: 'Validée',
  partial: 'Payée partiellement',
  paid: 'Soldée',
  dispute: 'Litige',
};
const PAYMENT_MODE_LABELS = {
  virement: 'Virement', prelevement: 'Prélèvement', cheque: 'Chèque',
  cb: 'Carte bancaire', especes: 'Espèces', lcr: 'LCR/Traite', autre: 'Autre',
};

// Total dû (TTC) d'une facture après prise en compte des paiements/avoirs/escomptes
function invoiceAmountDue(inv) {
  const total = inv.totalTtc != null ? inv.totalTtc : (inv.total != null ? inv.total * 1.20 : 0);
  const paid = (inv.payments || []).reduce((sum, p) => sum + (parseFloat(p.amount) || 0), 0);
  return Math.max(0, total - paid);
}

function invoiceTotalPaid(inv) {
  return (inv.payments || []).reduce((sum, p) => sum + (parseFloat(p.amount) || 0), 0);
}

function invoiceTotalTtc(inv) {
  if (inv.totalTtc != null) return inv.totalTtc;
  if (inv.total != null) return inv.total * 1.20;
  return 0;
}

// Détermine le statut auto en fonction des paiements (sauf si en litige ou à vérifier manuellement)
function autoStatus(inv) {
  if (inv.status === 'dispute') return 'dispute';
  if (inv.status === 'to_check') return 'to_check';
  const total = invoiceTotalTtc(inv);
  const paid = invoiceTotalPaid(inv);
  if (total <= 0) return inv.status || 'validated';
  if (paid >= total - 0.01) return 'paid';
  if (paid > 0) return 'partial';
  return inv.status || 'validated';
}

// Nombre de jours entre la date d'échéance et aujourd'hui (positif = en retard)
function daysOverdue(inv) {
  if (!inv.dueDate) return null;
  const due = new Date(inv.dueDate);
  const now = new Date(); now.setHours(0,0,0,0);
  return Math.floor((now - due) / 86400000);
}

// Catégorie balance âgée d'une facture non soldée (tranches 0-30 / 31-60 / 61-90 / +90)
function agedCategory(inv) {
  const due = invoiceAmountDue(inv);
  if (due <= 0.01) return null;
  const od = daysOverdue(inv);
  if (od == null || od < 0) return 'future'; // pas encore échue
  if (od <= 30) return 'aged_30';
  if (od <= 60) return 'aged_60';
  if (od <= 90) return 'aged_90';
  return 'aged_90'; // +90 (on garde la même clé pour simplifier)
}

// ----- ENREGISTREMENT D'UN PAIEMENT -----
let payingInvoiceId = null;

function openPaymentModal(invoiceId) {
  payingInvoiceId = invoiceId;
  const inv = STATE.invoices[invoiceId];
  if (!inv) return;
  const due = invoiceAmountDue(inv);
  const total = invoiceTotalTtc(inv);
  const paid = invoiceTotalPaid(inv);

  $('#paymentModalTitle').textContent = `Règlement — ${inv.supplier} · ${inv.number || '—'}`;
  $('#paymentSummary').innerHTML = `
    <div class="ps-item"><label>Montant TTC</label><value>${fmtMoney(total)}</value></div>
    <div class="ps-item"><label>Déjà réglé</label><value class="ok">${fmtMoney(paid)}</value></div>
    <div class="ps-item"><label>Reste dû</label><value class="${due > 0 ? 'danger' : 'ok'}">${fmtMoney(due)}</value></div>
  `;
  $('#payType').value = 'payment';
  $('#payDate').value = today();
  $('#payAmount').value = due > 0 ? due.toFixed(2) : '';
  $('#payMode').value = inv.paymentMode || 'virement';
  $('#payReference').value = '';
  $('#payNotes').value = '';
  $('#paymentModal').hidden = false;
}

async function savePayment() {
  if (!payingInvoiceId) return;
  const inv = STATE.invoices[payingInvoiceId];
  if (!inv) return;

  const amount = parseFrNumber($('#payAmount').value);
  if (isNaN(amount) || amount <= 0) { toast('Montant invalide', 'warn'); return; }

  const payment = {
    id: uid(),
    type: $('#payType').value,
    date: $('#payDate').value || today(),
    amount: amount,
    mode: $('#payMode').value,
    reference: $('#payReference').value.trim(),
    notes: $('#payNotes').value.trim(),
    createdAt: Date.now(),
  };
  inv.payments = inv.payments || [];
  inv.payments.push(payment);
  // Recalcul du statut (si pas en litige)
  if (inv.status !== 'dispute' && inv.status !== 'to_check') {
    inv.status = autoStatus(inv);
  } else if (inv.status === 'to_check' && invoiceTotalPaid(inv) > 0) {
    // si on enregistre un paiement, on suppose que c'est validé
    inv.status = autoStatus({ ...inv, status: 'validated' });
  }
  await fbWrite('invoices/' + payingInvoiceId, inv);
  toast('Règlement enregistré', 'ok');
  $('#paymentModal').hidden = true;
  renderAll();
  // Re-ouvrir la modale détail si elle était ouverte
  if (viewingInvoiceId === payingInvoiceId) openInvoiceModal(payingInvoiceId);
}

async function deletePayment(invoiceId, paymentId) {
  if (!confirm('Supprimer ce règlement ?')) return;
  const inv = STATE.invoices[invoiceId];
  if (!inv || !inv.payments) return;
  inv.payments = inv.payments.filter(p => p.id !== paymentId);
  inv.status = autoStatus(inv);
  await fbWrite('invoices/' + invoiceId, inv);
  toast('Règlement supprimé', 'ok');
  renderAll();
  if (viewingInvoiceId === invoiceId) openInvoiceModal(invoiceId);
}

async function changeInvoiceStatus(invoiceId, newStatus) {
  const inv = STATE.invoices[invoiceId];
  if (!inv) return;
  inv.status = newStatus;
  await fbWrite('invoices/' + invoiceId, inv);
  renderAll();
  if (viewingInvoiceId === invoiceId) openInvoiceModal(invoiceId);
}

// ============================================================
// RENDERING SUIVI FACTURIER
// ============================================================

function renderPayment() {
  const tbody = $('#paymentBody');
  const search = normalize($('#searchPayment').value);
  const fStatus = $('#paymentStatus').value;
  const fSup = $('#paymentSupplier').value;
  const fDue = $('#paymentDue').value;

  const items = Object.entries(STATE.invoices).map(([id, i]) => ({ id, ...i }));
  const filtered = items.filter(i => {
    const status = autoStatus(i);
    if (fStatus && status !== fStatus) return false;
    if (fSup && i.supplier !== fSup) return false;
    if (fDue) {
      const od = daysOverdue(i);
      const due = invoiceAmountDue(i);
      if (fDue === 'late' && (od == null || od <= 0 || due <= 0)) return false;
      if (fDue === 'week' && (od == null || od < -7 || od > 0 || due <= 0)) return false;
      if (fDue === 'month' && (od == null || od < -30 || od > 0 || due <= 0)) return false;
      if (fDue === 'future' && (od == null || od >= 0 || due <= 0)) return false;
    }
    if (search) {
      const hay = normalize([i.supplier, i.number].join(' '));
      if (!hay.includes(search)) return false;
    }
    return true;
  });

  if (filtered.length === 0) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="10">Aucune facture ne correspond aux filtres.</td></tr>`;
  } else {
    tbody.innerHTML = filtered
      .sort((a, b) => {
        // tri par échéance asc, sinon date desc
        const ad = a.dueDate || a.date || '0';
        const bd = b.dueDate || b.date || '0';
        return ad.localeCompare(bd);
      })
      .map(i => {
        const status = autoStatus(i);
        const total = invoiceTotalTtc(i);
        const paid = invoiceTotalPaid(i);
        const due = invoiceAmountDue(i);
        const od = daysOverdue(i);
        let dueClass = '';
        if (due > 0 && od != null) {
          if (od > 0) dueClass = 'due-late';
          else if (od >= -7) dueClass = 'due-soon';
        }
        const stats = invoiceStats(i);
        let ecartCell = '<span class="muted">—</span>';
        if (stats.danger > 0) ecartCell = `<span class="badge badge-danger">${stats.danger}</span>`;
        else if (stats.warn > 0) ecartCell = `<span class="badge badge-warn">${stats.warn}</span>`;
        else if (stats.ok > 0) ecartCell = `<span class="badge badge-ok">OK</span>`;

        return `
          <tr class="${dueClass}" data-id="${i.id}">
            <td><span class="status-badge status-${status}">${STATUS_LABELS[status]}</span></td>
            <td>${fmtDate(i.date)}</td>
            <td>${i.dueDate ? fmtDate(i.dueDate) : '<span class="muted">—</span>'}${od != null && od > 0 && due > 0 ? ` <span class="muted small">(+${od}j)</span>` : ''}</td>
            <td>${escapeHtml(i.supplier || '—')}</td>
            <td><strong>${escapeHtml(i.number || '—')}</strong></td>
            <td class="numeric">${fmtMoney(total)}</td>
            <td class="numeric ${paid > 0 ? 'text-ok' : ''}">${fmtMoney(paid)}</td>
            <td class="numeric ${due > 0 ? 'text-danger' : 'text-ok'}"><strong>${fmtMoney(due)}</strong></td>
            <td>${ecartCell}</td>
            <td>
              ${due > 0 ? `<button class="btn-mini" data-pay="${i.id}">Régler</button>` : ''}
              <button class="btn-mini" data-view="${i.id}">Voir</button>
            </td>
          </tr>`;
      }).join('');

    tbody.querySelectorAll('[data-pay]').forEach(b =>
      b.addEventListener('click', e => { e.stopPropagation(); openPaymentModal(b.dataset.pay); }));
    tbody.querySelectorAll('[data-view]').forEach(b =>
      b.addEventListener('click', e => { e.stopPropagation(); openInvoiceModal(b.dataset.view); }));
  }

  renderPaymentKpi();
}

function renderPaymentKpi() {
  const now = new Date(); now.setHours(0,0,0,0);
  const weekEnd = new Date(now); weekEnd.setDate(weekEnd.getDate() + 7);
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);

  let totalDue = 0, weekAmount = 0, weekCount = 0;
  let lateAmount = 0, lateCount = 0, checkCount = 0;
  let paidMonth = 0, paidMonthCount = 0;

  Object.values(STATE.invoices).forEach(inv => {
    const status = autoStatus(inv);
    const due = invoiceAmountDue(inv);
    const od = daysOverdue(inv);

    if (status === 'to_check') checkCount++;
    if (due > 0.01) {
      totalDue += due;
      if (od != null && od > 0) { lateAmount += due; lateCount++; }
      else if (inv.dueDate) {
        const d = new Date(inv.dueDate);
        if (d <= weekEnd && d >= now) { weekAmount += due; weekCount++; }
      }
    }
    // Soldé ce mois (dernier paiement dans le mois en cours, statut paid)
    if (status === 'paid' && inv.payments && inv.payments.length) {
      const last = inv.payments[inv.payments.length - 1];
      if (last.date && new Date(last.date) >= monthStart) {
        paidMonth += invoiceTotalTtc(inv);
        paidMonthCount++;
      }
    }
  });

  $('#kpiDue').textContent = fmtMoney(totalDue);
  $('#kpiWeek').textContent = fmtMoney(weekAmount);
  $('#kpiWeekCount').textContent = `${weekCount} facture${weekCount > 1 ? 's' : ''}`;
  $('#kpiLate').textContent = fmtMoney(lateAmount);
  $('#kpiLateCount').textContent = `${lateCount} facture${lateCount > 1 ? 's' : ''}`;
  $('#kpiCheck').textContent = checkCount;
  $('#kpiPaid').textContent = fmtMoney(paidMonth);
  $('#kpiPaidCount').textContent = `${paidMonthCount} facture${paidMonthCount > 1 ? 's' : ''}`;
}

// ============================================================
// RENDERING COMPTES FOURNISSEURS
// ============================================================

let selectedAccountSupplier = null;

function renderAccounts() {
  // Sidebar : liste des fournisseurs avec leur solde
  const search = normalize($('#searchAccountSup').value);
  const sups = Object.values(STATE.suppliers);

  // Pour chaque fournisseur, calculer le solde
  const balances = sups.map(s => {
    const invs = Object.values(STATE.invoices).filter(i => i.supplier === s.name);
    const balance = invs.reduce((sum, i) => sum + invoiceAmountDue(i), 0);
    const lateInvs = invs.filter(i => {
      const od = daysOverdue(i);
      return invoiceAmountDue(i) > 0 && od != null && od > 0;
    });
    return { ...s, balance, hasLate: lateInvs.length > 0, invCount: invs.length };
  }).filter(s => !search || normalize(s.name).includes(search))
    .sort((a, b) => b.balance - a.balance || a.name.localeCompare(b.name));

  const list = $('#accountList');
  if (balances.length === 0) {
    list.innerHTML = `<div class="empty-card" style="padding:24px;font-size:12px">Aucun fournisseur.</div>`;
  } else {
    list.innerHTML = balances.map(s => `
      <div class="account-item ${s.balance <= 0.01 ? 'zero' : ''} ${s.hasLate ? 'late' : ''}"
           data-name="${escapeAttr(s.name)}">
        <span class="account-name">${escapeHtml(s.name)}</span>
        <span class="account-balance">${fmtMoney(s.balance)}</span>
      </div>
    `).join('');
    list.querySelectorAll('.account-item').forEach(el =>
      el.addEventListener('click', () => selectAccount(el.dataset.name)));
  }

  // Si pas de sélection, prendre le premier
  if (!selectedAccountSupplier && balances.length) {
    selectedAccountSupplier = balances[0].name;
  }
  // Re-mettre la classe active
  list.querySelectorAll('.account-item').forEach(el =>
    el.classList.toggle('active', el.dataset.name === selectedAccountSupplier));

  // Détail du compte
  if (selectedAccountSupplier) renderAccountDetail(selectedAccountSupplier);
  else $('#accountDetail').innerHTML = `<div class="empty-card" style="padding:48px">Sélectionne un fournisseur.</div>`;

  // KPI globaux
  renderAccountsKpi();
}

function selectAccount(name) {
  selectedAccountSupplier = name;
  renderAccounts();
}

function renderAccountDetail(name) {
  const supplier = Object.values(STATE.suppliers).find(s => s.name === name);
  const invs = Object.entries(STATE.invoices)
    .map(([id, i]) => ({ id, ...i }))
    .filter(i => i.supplier === name);

  const balance = invs.reduce((sum, i) => sum + invoiceAmountDue(i), 0);

  // Balance âgée pour ce fournisseur
  const aged = { future: 0, aged_30: 0, aged_60: 0, aged_90: 0 };
  invs.forEach(i => {
    const cat = agedCategory(i);
    if (cat) aged[cat] += invoiceAmountDue(i);
  });
  const nonEchue = aged.future;
  const total = aged.future + aged.aged_30 + aged.aged_60 + aged.aged_90;

  // Toutes les factures triées par date desc
  const allInvs = invs.sort((a, b) => (b.date || '0').localeCompare(a.date || '0'));

  const allInvsHtml = allInvs.length ? `
    <div class="table-wrap" style="max-height:420px">
      <table class="data-table">
        <thead><tr>
          <th>Statut</th><th>Date</th><th>Échéance</th><th>N°</th>
          <th>TTC</th><th>Payé</th><th>Reste</th><th></th>
        </tr></thead>
        <tbody>${allInvs.map(i => {
          const status = autoStatus(i);
          const due = invoiceAmountDue(i);
          const od = daysOverdue(i);
          let dueClass = '';
          if (due > 0 && od != null && od > 0) dueClass = 'due-late';
          return `
            <tr class="${dueClass}">
              <td><span class="status-badge status-${status}">${STATUS_LABELS[status]}</span></td>
              <td>${fmtDate(i.date)}</td>
              <td>${fmtDate(i.dueDate)}${od != null && od > 0 && due > 0 ? ` <span class="muted small">(+${od}j)</span>` : ''}</td>
              <td><strong>${escapeHtml(i.number || '—')}</strong></td>
              <td class="numeric">${fmtMoney(invoiceTotalTtc(i))}</td>
              <td class="numeric">${fmtMoney(invoiceTotalPaid(i))}</td>
              <td class="numeric ${due > 0 ? 'text-danger' : 'text-ok'}"><strong>${fmtMoney(due)}</strong></td>
              <td>
                ${due > 0 ? `<button class="btn-mini" data-pay="${i.id}">Régler</button>` : ''}
                <button class="btn-mini" data-view="${i.id}">Voir</button>
              </td>
            </tr>`;
        }).join('')}</tbody>
      </table>
    </div>` : '<div class="empty-card" style="padding:20px;font-size:12px">Aucune facture pour ce fournisseur.</div>';

  $('#accountDetail').innerHTML = `
    <div class="account-header">
      <div>
        <h3>${escapeHtml(name)}</h3>
        <div class="meta">${escapeHtml(supplier?.address || '—')}</div>
        <div class="meta">${escapeHtml(supplier?.phone || '')} ${supplier?.email ? '· ' + escapeHtml(supplier.email) : ''}</div>
        <div class="meta">${invs.length} facture(s) · délai ${supplier?.payment || 30}j${supplier?.discount ? ' · −' + supplier.discount + '% remise' : ''}</div>
      </div>
      <div class="account-balance-big">
        <div class="label">Solde dû</div>
        <div class="value ${balance <= 0.01 ? 'zero' : ''}">${fmtMoney(balance)}</div>
      </div>
    </div>

    <div class="aged-balance">
      <div class="aged-cell ${nonEchue > 0 ? 'has-amount aged-0' : ''}">
        <div class="label">Non échu</div>
        <div class="amount">${fmtMoney(nonEchue)}</div>
      </div>
      <div class="aged-cell ${aged.aged_30 > 0 ? 'has-amount aged-30' : ''}">
        <div class="label">0-30 j</div>
        <div class="amount">${fmtMoney(aged.aged_30)}</div>
      </div>
      <div class="aged-cell ${aged.aged_60 > 0 ? 'has-amount aged-60' : ''}">
        <div class="label">31-60 j</div>
        <div class="amount">${fmtMoney(aged.aged_60)}</div>
      </div>
      <div class="aged-cell ${aged.aged_90 > 0 ? 'has-amount aged-90' : ''}">
        <div class="label">+90 j</div>
        <div class="amount">${fmtMoney(aged.aged_90)}</div>
      </div>
      <div class="aged-cell">
        <div class="label">Total dû</div>
        <div class="amount">${fmtMoney(total)}</div>
      </div>
    </div>

    <div class="account-section">
      <h4>Toutes les factures (${allInvs.length})</h4>
      ${allInvsHtml}
    </div>
  `;

  $('#accountDetail').querySelectorAll('[data-pay]').forEach(b =>
    b.addEventListener('click', () => openPaymentModal(b.dataset.pay)));
  $('#accountDetail').querySelectorAll('[data-view]').forEach(b =>
    b.addEventListener('click', () => openInvoiceModal(b.dataset.view)));
}

function renderAccountsKpi() {
  const aged = { future: 0, aged_30: 0, aged_60: 0, aged_90: 0 };
  let total = 0;
  Object.values(STATE.invoices).forEach(i => {
    const cat = agedCategory(i);
    const due = invoiceAmountDue(i);
    if (cat) aged[cat] += due;
    total += due;
  });
  $('#kpiAccTotal').textContent = fmtMoney(total);
  $('#kpiAcc0').textContent = fmtMoney(aged.future);
  $('#kpiAcc30').textContent = fmtMoney(aged.aged_30);
  $('#kpiAcc60').textContent = fmtMoney(aged.aged_60);
  $('#kpiAcc90').textContent = fmtMoney(aged.aged_90);
}

// ============================================================
// EXPORTS — paiements & balance
// ============================================================

function exportPaymentXlsx() {
  const rows = Object.values(STATE.invoices).map(i => {
    const status = autoStatus(i);
    const total = invoiceTotalTtc(i);
    const paid = invoiceTotalPaid(i);
    const due = invoiceAmountDue(i);
    const od = daysOverdue(i);
    return {
      Statut: STATUS_LABELS[status],
      Date: i.date,
      Échéance: i.dueDate || '',
      'Jours retard': od != null && od > 0 && due > 0 ? od : '',
      Fournisseur: i.supplier,
      'N° facture': i.number,
      'Total HT': i.total ?? '',
      TVA: i.vat ?? '',
      'Total TTC': total,
      'Mode paiement': PAYMENT_MODE_LABELS[i.paymentMode] || i.paymentMode || '',
      Payé: paid,
      'Reste dû': due,
    };
  });
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Suivi facturier');
  XLSX.writeFile(wb, `suivi_facturier_${today()}.xlsx`);
}

function exportAccountsXlsx() {
  const sups = Object.values(STATE.suppliers);
  const rows = sups.map(s => {
    const invs = Object.values(STATE.invoices).filter(i => i.supplier === s.name);
    const aged = { future: 0, aged_30: 0, aged_60: 0, aged_90: 0 };
    invs.forEach(i => {
      const cat = agedCategory(i);
      if (cat) aged[cat] += invoiceAmountDue(i);
    });
    const total = aged.future + aged.aged_30 + aged.aged_60 + aged.aged_90;
    return {
      Fournisseur: s.name,
      'Délai paiement (j)': s.payment || 30,
      Factures: invs.length,
      'Non échu': aged.future,
      '0-30 j': aged.aged_30,
      '31-60 j': aged.aged_60,
      '+60 j': aged.aged_90,
      'Solde total': total,
    };
  }).filter(r => r.Factures > 0)
    .sort((a, b) => b['Solde total'] - a['Solde total']);
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Balance fournisseurs');
  XLSX.writeFile(wb, `balance_fournisseurs_${today()}.xlsx`);
}

// ============================================================
// RENDERING
// ============================================================

function renderTabs() {
  $$('.nav-btn').forEach(b => b.addEventListener('click', () => {
    $$('.nav-btn').forEach(x => x.classList.remove('active'));
    $$('.tab-panel').forEach(p => p.classList.remove('active'));
    b.classList.add('active');
    $('#tab-' + b.dataset.tab).classList.add('active');
  }));
}

// ----- ARTICLES -----
function renderArticles() {
  const tbody = $('#articlesBody');
  const search = normalize($('#searchArticles').value);
  const filterSup = $('#filterSupplier').value;
  const filterCat = $('#filterCategory').value;

  const items = Object.entries(STATE.articles).map(([id, a]) => ({ id, ...a }));
  const filtered = items.filter(a => {
    if (filterSup && a.supplier !== filterSup) return false;
    if (filterCat && a.category !== filterCat) return false;
    if (search) {
      const hay = normalize([a.code, a.designation, a.supplier, a.supplierCode].join(' '));
      if (!hay.includes(search)) return false;
    }
    return true;
  });

  $('#articlesCount').textContent = `${filtered.length} article${filtered.length > 1 ? 's' : ''}`;

  if (filtered.length === 0) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="9">Aucun article — clique « Nouvel article » ou importe depuis Excel.</td></tr>`;
    return;
  }

  tbody.innerHTML = filtered
    .sort((a, b) => (a.code || '').localeCompare(b.code || ''))
    .map(a => `
      <tr data-id="${a.id}">
        <td><strong>${escapeHtml(a.code || '—')}</strong>${a.supplierCode ? `<br><span class="muted small">${escapeHtml(a.supplierCode)}</span>` : ''}</td>
        <td>${escapeHtml(a.designation || '—')}</td>
        <td>${escapeHtml(a.supplier || '—')}</td>
        <td><span class="badge badge-muted">${escapeHtml(a.category || '—')}</span></td>
        <td>${escapeHtml(a.unit || 'U')}</td>
        <td class="numeric">${fmtMoney(a.price)}</td>
        <td class="numeric">${fmtDate(a.updatedAt)}</td>
        <td class="numeric">${a.tolerance != null && a.tolerance !== '' ? a.tolerance + ' %' : '<span class="muted">défaut</span>'}</td>
        <td><button class="btn-mini" data-edit="${a.id}">Modifier</button></td>
      </tr>
    `).join('');

  tbody.querySelectorAll('[data-edit]').forEach(btn =>
    btn.addEventListener('click', () => openArticleModal(btn.dataset.edit)));
}

// ----- FOURNISSEURS -----
function renderSuppliers() {
  const grid = $('#suppliersGrid');
  const items = Object.entries(STATE.suppliers).map(([id, s]) => ({ id, ...s }));

  if (items.length === 0) {
    grid.innerHTML = `<div class="empty-card">Aucun fournisseur — ajoute-en un pour commencer.</div>`;
    return;
  }

  grid.innerHTML = items
    .sort((a, b) => (a.name || '').localeCompare(b.name || ''))
    .map(s => {
      const articleCount = Object.values(STATE.articles).filter(a => a.supplier === s.name).length;
      const invoiceCount = Object.values(STATE.invoices).filter(i => i.supplier === s.name).length;
      return `
        <div class="supplier-card" data-id="${s.id}">
          <h4>${escapeHtml(s.name)}</h4>
          <div class="meta">${escapeHtml(s.address || '—')}</div>
          <div class="meta">${escapeHtml(s.phone || '')} ${s.email ? '· ' + escapeHtml(s.email) : ''}</div>
          <div class="stats">
            <span><strong>${articleCount}</strong> articles</span>
            <span><strong>${invoiceCount}</strong> factures</span>
            ${s.discount ? `<span><strong>−${s.discount}%</strong> remise</span>` : ''}
          </div>
        </div>`;
    }).join('');

  grid.querySelectorAll('.supplier-card').forEach(c =>
    c.addEventListener('click', () => openSupplierModal(c.dataset.id)));
}

function renderSuppliersDatalists() {
  const names = [...new Set(Object.values(STATE.suppliers).map(s => s.name))].sort();
  const html = names.map(n => `<option value="${escapeAttr(n)}">`).join('');
  ['supplierList', 'supplierList2'].forEach(id => {
    const el = $('#' + id); if (el) el.innerHTML = html;
  });
  // Filter dropdowns
  ['filterSupplier', 'ecartSupplier', 'histSupplier', 'paymentSupplier'].forEach(id => {
    const el = $('#' + id); if (!el) return;
    const cur = el.value;
    el.innerHTML = `<option value="">Tous fournisseurs</option>` + names.map(n => `<option value="${escapeAttr(n)}">${escapeHtml(n)}</option>`).join('');
    el.value = cur;
  });
}

// ----- INVOICES -----
function renderInvoices() {
  const tbody = $('#invoicesBody');
  const search = normalize($('#searchInvoices').value);
  const filterSup = $('#histSupplier').value;
  const month = $('#histMonth').value;

  const items = Object.entries(STATE.invoices).map(([id, i]) => ({ id, ...i }));
  const filtered = items.filter(i => {
    if (filterSup && i.supplier !== filterSup) return false;
    if (month && (!i.date || !i.date.startsWith(month))) return false;
    if (search) {
      const hay = normalize([i.supplier, i.number].join(' '));
      if (!hay.includes(search)) return false;
    }
    return true;
  });

  if (filtered.length === 0) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="8">Aucune facture enregistrée.</td></tr>`;
    return;
  }

  tbody.innerHTML = filtered
    .sort((a, b) => (b.date || '').localeCompare(a.date || ''))
    .map(i => {
      const stats = invoiceStats(i);
      let statusBadge = '<span class="badge badge-ok">Conforme</span>';
      if (stats.danger > 0) statusBadge = `<span class="badge badge-danger">${stats.danger} critique(s)</span>`;
      else if (stats.warn > 0) statusBadge = `<span class="badge badge-warn">${stats.warn} modéré(s)</span>`;
      else if (stats.noref > 0) statusBadge = `<span class="badge badge-muted">${stats.noref} non réf.</span>`;
      return `
        <tr data-id="${i.id}">
          <td>${fmtDate(i.date)}</td>
          <td>${escapeHtml(i.supplier || '—')}</td>
          <td><strong>${escapeHtml(i.number || '—')}</strong></td>
          <td class="numeric">${(i.lines || []).length}</td>
          <td class="numeric">${fmtMoney(i.total)}</td>
          <td>${stats.danger || stats.warn ? `<span class="text-${stats.danger ? 'danger' : 'warn'}">${fmtMoney(stats.overcost)}</span>` : '<span class="muted">—</span>'}</td>
          <td>${statusBadge}</td>
          <td><button class="btn-mini" data-view="${i.id}">Voir</button></td>
        </tr>
      `;
    }).join('');

  tbody.querySelectorAll('[data-view]').forEach(btn =>
    btn.addEventListener('click', () => openInvoiceModal(btn.dataset.view)));
}

function invoiceStats(invoice) {
  const stats = { ok: 0, warn: 0, danger: 0, noref: 0, overcost: 0 };
  (invoice.lines || []).forEach(l => {
    const m = findArticleMatch(l, invoice.supplier);
    const e = computeEcart(l, m ? m.article : null);
    stats[e.level]++;
    if (e.overcost > 0 && (e.level === 'warn' || e.level === 'danger')) stats.overcost += e.overcost;
  });
  return stats;
}

// ----- ÉCARTS DASHBOARD -----
function renderEcarts() {
  const tbody = $('#ecartsBody');
  const period = $('#ecartPeriod').value;
  const filterSup = $('#ecartSupplier').value;
  const level = $('#ecartLevel').value;

  const cutoff = period === 'all' ? null : Date.now() - parseInt(period) * 86400000;

  const rows = [];
  Object.entries(STATE.invoices).forEach(([invId, inv]) => {
    if (filterSup && inv.supplier !== filterSup) return;
    if (cutoff && inv.date && new Date(inv.date).getTime() < cutoff) return;
    (inv.lines || []).forEach((l, idx) => {
      const m = findArticleMatch(l, inv.supplier);
      const e = computeEcart(l, m ? m.article : null);
      if (level && e.level !== level) return;
      if (!level && (e.level === 'ok' || e.level === 'noref')) return; // par défaut on n'affiche que warn/danger
      rows.push({ inv, line: l, match: m, ecart: e });
    });
  });

  if (rows.length === 0) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="11">Aucun écart à afficher.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .sort((a, b) => (b.ecart.overcost || 0) - (a.ecart.overcost || 0))
    .map(r => `
      <tr class="row-${r.ecart.level}">
        <td>${fmtDate(r.inv.date)}</td>
        <td>${escapeHtml(r.inv.supplier || '—')}</td>
        <td>${escapeHtml(r.inv.number || '—')}</td>
        <td><strong>${escapeHtml(r.line.code || '—')}</strong></td>
        <td>${escapeHtml(r.line.designation || '—')}</td>
        <td class="numeric">${fmt(r.line.qty)} ${escapeHtml(r.line.unit || '')}</td>
        <td class="numeric">${fmtMoney(r.line.pu)}</td>
        <td class="numeric">${r.match ? fmtMoney(r.match.article.price) : '<span class="muted">—</span>'}</td>
        <td class="numeric cell-ecart">${fmtPct(r.ecart.pct)}</td>
        <td class="numeric">${r.ecart.overcost > 0 ? fmtMoney(r.ecart.overcost) : '—'}</td>
        <td><button class="btn-mini" data-view="${r.inv.id}">Voir</button></td>
      </tr>
    `).join('');

  tbody.querySelectorAll('[data-view]').forEach(btn =>
    btn.addEventListener('click', () => openInvoiceModal(btn.dataset.view)));
}

// ----- KPI -----
function renderKPI() {
  const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - 30);
  let invCount = 0, invMonth = 0, ok = 0, warn = 0, danger = 0, overcost = 0;
  Object.values(STATE.invoices).forEach(inv => {
    invCount++;
    if (inv.date && new Date(inv.date) >= cutoff) invMonth++;
    (inv.lines || []).forEach(l => {
      const m = findArticleMatch(l, inv.supplier);
      const e = computeEcart(l, m ? m.article : null);
      if (e.level === 'ok') ok++;
      else if (e.level === 'warn') { warn++; if (e.overcost > 0) overcost += e.overcost; }
      else if (e.level === 'danger') { danger++; if (e.overcost > 0) overcost += e.overcost; }
    });
  });
  $('#kpiInvoices').textContent = invCount;
  $('#kpiInvoicesSub').textContent = `${invMonth} ce mois`;
  $('#kpiOk').textContent = ok;
  $('#kpiWarn').textContent = warn;
  $('#kpiDanger').textContent = danger;
  $('#kpiOvercost').textContent = fmtMoney(overcost);
}

// ============================================================
// MODAL ARTICLE
// ============================================================

let editingArticleId = null;

function openArticleModal(id) {
  editingArticleId = id || null;
  const a = id ? STATE.articles[id] : null;
  $('#articleModalTitle').textContent = a ? 'Modifier article' : 'Nouvel article';
  $('#aCode').value = a?.code || '';
  $('#aSupplierCode').value = a?.supplierCode || '';
  $('#aDesignation').value = a?.designation || '';
  $('#aSupplier').value = a?.supplier || '';
  $('#aCategory').value = a?.category || 'plomberie';
  $('#aUnit').value = a?.unit || 'U';
  $('#aPrice').value = a?.price ?? '';
  $('#aTolerance').value = a?.tolerance ?? '';
  $('#aNotes').value = a?.notes || '';
  $('#btnDeleteArticle').hidden = !a;
  $('#articleModal').hidden = false;
}

async function saveArticle() {
  const id = editingArticleId || uid();
  const a = {
    code: $('#aCode').value.trim(),
    supplierCode: $('#aSupplierCode').value.trim(),
    designation: $('#aDesignation').value.trim(),
    supplier: $('#aSupplier').value.trim(),
    category: $('#aCategory').value,
    unit: $('#aUnit').value,
    price: parseFrNumber($('#aPrice').value),
    tolerance: $('#aTolerance').value.trim() === '' ? null : parseFrNumber($('#aTolerance').value),
    notes: $('#aNotes').value.trim(),
    updatedAt: Date.now(),
  };
  if (!a.designation || isNaN(a.price)) {
    toast('Désignation et prix de référence requis', 'warn');
    return;
  }
  STATE.articles[id] = a;
  await fbWrite('articles/' + id, a);
  $('#articleModal').hidden = true;
  toast('Article enregistré', 'ok');
  renderArticles();
}

async function deleteArticle() {
  if (!editingArticleId) return;
  if (!confirm('Supprimer cet article ?')) return;
  delete STATE.articles[editingArticleId];
  await fbRemove('articles/' + editingArticleId);
  $('#articleModal').hidden = true;
  toast('Article supprimé', 'ok');
  renderArticles();
}

// ============================================================
// MODAL FOURNISSEUR
// ============================================================

let editingSupplierId = null;

function openSupplierModal(id) {
  editingSupplierId = id || null;
  const s = id ? STATE.suppliers[id] : null;
  $('#supplierModalTitle').textContent = s ? 'Modifier fournisseur' : 'Nouveau fournisseur';
  $('#sName').value = s?.name || '';
  $('#sCode').value = s?.code || '';
  $('#sSiret').value = s?.siret || '';
  $('#sAddress').value = s?.address || '';
  $('#sPhone').value = s?.phone || '';
  $('#sEmail').value = s?.email || '';
  $('#sPayment').value = s?.payment ?? 30;
  $('#sDiscount').value = s?.discount ?? 0;
  $('#sNotes').value = s?.notes || '';
  $('#btnDeleteSupplier').hidden = !s;
  $('#supplierModal').hidden = false;
}

async function saveSupplier() {
  const id = editingSupplierId || uid();
  const s = {
    name: $('#sName').value.trim(),
    code: $('#sCode').value.trim(),
    siret: $('#sSiret').value.trim(),
    address: $('#sAddress').value.trim(),
    phone: $('#sPhone').value.trim(),
    email: $('#sEmail').value.trim(),
    payment: parseInt($('#sPayment').value) || 30,
    discount: parseFloat($('#sDiscount').value) || 0,
    notes: $('#sNotes').value.trim(),
  };
  if (!s.name) { toast('Nom requis', 'warn'); return; }
  STATE.suppliers[id] = s;
  await fbWrite('suppliers/' + id, s);
  $('#supplierModal').hidden = true;
  toast('Fournisseur enregistré', 'ok');
  renderSuppliers(); renderSuppliersDatalists();
}

async function deleteSupplier() {
  if (!editingSupplierId) return;
  if (!confirm('Supprimer ce fournisseur ?')) return;
  delete STATE.suppliers[editingSupplierId];
  await fbRemove('suppliers/' + editingSupplierId);
  $('#supplierModal').hidden = true;
  toast('Fournisseur supprimé', 'ok');
  renderSuppliers(); renderSuppliersDatalists();
}

// ============================================================
// MODAL DÉTAIL FACTURE
// ============================================================

let viewingInvoiceId = null;

function openInvoiceModal(id) {
  viewingInvoiceId = id;
  const inv = STATE.invoices[id];
  if (!inv) return;
  $('#invoiceModalTitle').textContent = `Facture ${inv.number || '—'} · ${inv.supplier || ''}`;
  const stats = invoiceStats(inv);
  const status = autoStatus(inv);
  const total = invoiceTotalTtc(inv);
  const paid = invoiceTotalPaid(inv);
  const due = invoiceAmountDue(inv);
  const od = daysOverdue(inv);

  const linesHtml = (inv.lines || []).map(l => {
    const m = findArticleMatch(l, inv.supplier);
    const e = computeEcart(l, m ? m.article : null);
    let badge = '';
    if (e.level === 'ok') badge = '<span class="badge badge-ok">OK</span>';
    else if (e.level === 'warn') badge = `<span class="badge badge-warn">${fmtPct(e.pct)}</span>`;
    else if (e.level === 'danger') badge = `<span class="badge badge-danger">${fmtPct(e.pct)}</span>`;
    else badge = '<span class="badge badge-muted">non réf.</span>';
    return `
      <tr class="row-${e.level}">
        <td><strong>${escapeHtml(l.code || '—')}</strong></td>
        <td>${escapeHtml(l.designation || '—')}</td>
        <td class="numeric">${fmt(l.qty)} ${escapeHtml(l.unit || '')}</td>
        <td class="numeric">${fmtMoney(l.pu)}</td>
        <td class="numeric">${m ? fmtMoney(m.article.price) : '<span class="muted">—</span>'}</td>
        <td>${badge}</td>
        <td class="numeric">${e.overcost > 0 ? fmtMoney(e.overcost) : '—'}</td>
      </tr>`;
  }).join('');

  // Paiements
  const paymentsHtml = (inv.payments && inv.payments.length) ? inv.payments.map(p => {
    const sign = p.type === 'payment' ? '−' : p.type === 'credit' ? '−' : '−'; // tous réduisent le dû
    return `
      <div class="payment-item ${p.type}">
        <div class="schedule-date">${fmtDate(p.date)}</div>
        <div>
          <strong>${p.type === 'payment' ? 'Paiement' : p.type === 'credit' ? 'Avoir/remboursement' : 'Escompte/remise'}</strong>
          <span class="muted small"> · ${PAYMENT_MODE_LABELS[p.mode] || p.mode}${p.reference ? ' · réf. ' + escapeHtml(p.reference) : ''}</span>
          ${p.notes ? `<div class="muted small">${escapeHtml(p.notes)}</div>` : ''}
        </div>
        <span class="payment-amount ${p.type}">${sign} ${fmtMoney(p.amount)}</span>
        <button class="btn-mini" data-delpay="${p.id}">×</button>
      </div>`;
  }).join('') : '<div class="empty-card" style="padding:16px;font-size:12px">Aucun règlement enregistré.</div>';

  // Sélecteur de statut
  const statusOptions = Object.entries(STATUS_LABELS)
    .map(([k, v]) => `<option value="${k}" ${status === k ? 'selected' : ''}>${v}</option>`).join('');

  $('#invoiceModalBody').innerHTML = `
    <div class="invoice-actions-bar">
      <div class="field" style="margin:0;flex:1;min-width:160px">
        <label>Statut</label>
        <select class="status-select" id="invStatusChange">${statusOptions}</select>
      </div>
      ${due > 0 ? `<button class="btn btn-primary" id="invBtnPay">+ Enregistrer un règlement</button>` : ''}
    </div>

    <div class="invoice-detail-meta">
      <div class="meta-item"><label>Date facture</label><value>${fmtDate(inv.date)}</value></div>
      <div class="meta-item"><label>Échéance</label><value class="${od != null && od > 0 && due > 0 ? 'text-danger' : ''}">${fmtDate(inv.dueDate)}${od != null && od > 0 && due > 0 ? ` (+${od}j)` : ''}</value></div>
      <div class="meta-item"><label>Mode paiement</label><value>${PAYMENT_MODE_LABELS[inv.paymentMode] || '—'}</value></div>
      <div class="meta-item"><label>Total HT</label><value>${fmtMoney(inv.total)}</value></div>
      <div class="meta-item"><label>TVA</label><value>${fmtMoney(inv.vat)}</value></div>
      <div class="meta-item"><label>Total TTC</label><value><strong>${fmtMoney(total)}</strong></value></div>
      <div class="meta-item"><label>Réglé</label><value class="text-ok">${fmtMoney(paid)}</value></div>
      <div class="meta-item"><label>Reste dû</label><value class="${due > 0 ? 'text-danger' : 'text-ok'}"><strong>${fmtMoney(due)}</strong></value></div>
      <div class="meta-item"><label>Lignes / Conf. / Mod. / Crit.</label><value>${(inv.lines || []).length} · <span class="text-ok">${stats.ok}</span> · <span class="text-warn">${stats.warn}</span> · <span class="text-danger">${stats.danger}</span></value></div>
    </div>

    <div class="account-section" style="margin-top:8px">
      <h4>Règlements</h4>
      <div class="payments-list">${paymentsHtml}</div>
    </div>

    <div class="account-section">
      <h4>Lignes article (${(inv.lines || []).length})</h4>
      <div class="table-wrap">
        <table class="data-table">
          <thead><tr><th>Code</th><th>Désignation</th><th>Qté</th><th>PU facturé</th><th>PU réf.</th><th>Statut</th><th>Surcoût</th></tr></thead>
          <tbody>${linesHtml || '<tr class="empty-row"><td colspan="7">Aucune ligne.</td></tr>'}</tbody>
        </table>
      </div>
    </div>
  `;

  // Listeners
  const statusSel = $('#invStatusChange');
  if (statusSel) statusSel.addEventListener('change', e => changeInvoiceStatus(id, e.target.value));
  const payBtn = $('#invBtnPay');
  if (payBtn) payBtn.addEventListener('click', () => openPaymentModal(id));
  $('#invoiceModalBody').querySelectorAll('[data-delpay]').forEach(b =>
    b.addEventListener('click', () => deletePayment(id, b.dataset.delpay)));

  $('#invoiceModal').hidden = false;
}

async function deleteInvoice() {
  if (!viewingInvoiceId) return;
  if (!confirm('Supprimer cette facture ?')) return;
  delete STATE.invoices[viewingInvoiceId];
  await fbRemove('invoices/' + viewingInvoiceId);
  $('#invoiceModal').hidden = true;
  toast('Facture supprimée', 'ok');
  renderInvoices(); renderEcarts(); renderKPI();
}

// ============================================================
// EXTRACT FLOW (Scan Tab)
// ============================================================

function resetCurrentInvoice() {
  STATE.currentInvoice = {
    supplier: '', number: '', date: today(), dueDate: '',
    total: null, vat: null, totalTtc: null,
    paymentMode: 'virement', lines: [],
  };
  $('#invSupplier').value = '';
  $('#invNumber').value = '';
  $('#invDate').value = today();
  $('#invDueDate').value = '';
  $('#invTotal').value = '';
  $('#invVat').value = '';
  $('#invTotalTtc').value = '';
  $('#invPaymentMode').value = 'virement';
  $('#linesBody').innerHTML = `<tr class="empty-row"><td colspan="9">Aucune ligne — dépose une facture pour commencer.</td></tr>`;
  $('#extractStatus').textContent = 'En attente';
  $('#extractStatus').className = 'chip chip-muted';
}

// Calcule la date d'échéance par défaut depuis la date facture + délai fournisseur
function defaultDueDate(invoiceDate, supplierName) {
  if (!invoiceDate) return '';
  let days = 30; // défaut
  for (const sid in STATE.suppliers) {
    const s = STATE.suppliers[sid];
    if (s.name && normalize(s.name) === normalize(supplierName)) {
      days = parseInt(s.payment) || 30;
      break;
    }
  }
  const d = new Date(invoiceDate);
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}

// Recalcule TVA et TTC à partir du HT et inversement
function recalcInvoiceTotals() {
  const ht = parseFrNumber($('#invTotal').value);
  const tva = parseFrNumber($('#invVat').value);
  const ttc = parseFrNumber($('#invTotalTtc').value);
  // si HT et TTC saisis -> calcul TVA
  if (!isNaN(ht) && !isNaN(ttc) && isNaN(tva)) {
    $('#invVat').value = (ttc - ht).toFixed(2);
  }
  // si HT et TVA saisis -> calcul TTC
  else if (!isNaN(ht) && !isNaN(tva) && isNaN(ttc)) {
    $('#invTotalTtc').value = (ht + tva).toFixed(2);
  }
  // si TTC seul saisi -> approximation TVA 20%
  else if (isNaN(ht) && !isNaN(ttc) && isNaN(tva)) {
    const htCalc = ttc / 1.20;
    $('#invTotal').value = htCalc.toFixed(2);
    $('#invVat').value = (ttc - htCalc).toFixed(2);
  }
}

async function handleFiles(files) {
  if (!files || !files.length) return;
  $('#extractStatus').textContent = 'Extraction…';
  $('#extractStatus').className = 'chip chip-info';

  // pour multi-fichiers : on concatène en une seule facture
  let allText = '';
  for (const f of files) {
    const t = await extractFromFile(f);
    allText += '\n' + t;
  }

  const parsed = parseInvoice(allText);
  STATE.currentInvoice = parsed;

  $('#invSupplier').value = parsed.supplier;
  $('#invNumber').value = parsed.number;
  $('#invDate').value = parsed.date || today();
  $('#invTotal').value = parsed.total ?? '';
  $('#invVat').value = parsed.vat ?? '';
  $('#invTotalTtc').value = parsed.totalTtc ?? '';
  $('#invDueDate').value = parsed.dueDate || defaultDueDate(parsed.date || today(), parsed.supplier);
  $('#invPaymentMode').value = parsed.paymentMode || 'virement';

  renderCurrentLines();
  $('#extractStatus').textContent = `${parsed.lines.length} ligne(s) détectée(s)`;
  $('#extractStatus').className = 'chip chip-ok';
  toast(`${parsed.lines.length} lignes détectées`, 'ok');
}

function renderCurrentLines() {
  const inv = STATE.currentInvoice;
  const body = $('#linesBody');
  if (!inv || !inv.lines.length) {
    body.innerHTML = `<tr class="empty-row"><td colspan="9">Aucune ligne — dépose une facture pour commencer.</td></tr>`;
    return;
  }

  body.innerHTML = inv.lines.map((l, i) => {
    const m = findArticleMatch(l, inv.supplier || $('#invSupplier').value);
    const e = computeEcart(l, m ? m.article : null);
    let ecartCell = '<span class="muted">—</span>';
    if (e.level === 'ok') ecartCell = `<span class="badge badge-ok">OK ${fmtPct(e.pct)}</span>`;
    else if (e.level === 'warn') ecartCell = `<span class="badge badge-warn">${fmtPct(e.pct)}</span>`;
    else if (e.level === 'danger') ecartCell = `<span class="badge badge-danger">${fmtPct(e.pct)}</span>`;
    else ecartCell = '<span class="badge badge-muted">non réf.</span>';

    return `
      <tr class="row-${e.level}" data-idx="${i}">
        <td><input type="text" data-f="code" value="${escapeAttr(l.code || '')}"></td>
        <td><input type="text" data-f="designation" value="${escapeAttr(l.designation || '')}"></td>
        <td><input type="number" step="0.01" class="numeric" data-f="qty" value="${l.qty ?? ''}"></td>
        <td><input type="text" data-f="unit" value="${escapeAttr(l.unit || 'U')}" style="width:50px"></td>
        <td><input type="number" step="0.0001" class="numeric" data-f="pu" value="${l.pu ?? ''}"></td>
        <td class="numeric">${l.qty && l.pu ? fmtMoney(l.qty * l.pu) : '—'}</td>
        <td>${m ? `<span class="muted small">${escapeHtml(m.article.code || '')} · ${fmtMoney(m.article.price)}</span>` : '<span class="muted small">—</span>'}</td>
        <td>${ecartCell}</td>
        <td><button class="btn-mini" data-del="${i}">×</button></td>
      </tr>
    `;
  }).join('');

  // listeners pour édition
  body.querySelectorAll('input').forEach(inp => {
    inp.addEventListener('change', e => {
      const tr = e.target.closest('tr');
      const idx = parseInt(tr.dataset.idx);
      const field = e.target.dataset.f;
      let val = e.target.value;
      if (field === 'qty' || field === 'pu') val = parseFrNumber(val);
      STATE.currentInvoice.lines[idx][field] = val;
      renderCurrentLines();
    });
  });
  body.querySelectorAll('[data-del]').forEach(btn =>
    btn.addEventListener('click', () => {
      STATE.currentInvoice.lines.splice(parseInt(btn.dataset.del), 1);
      renderCurrentLines();
    }));
}

async function saveCurrentInvoice() {
  const inv = STATE.currentInvoice;
  if (!inv) return;
  inv.supplier = $('#invSupplier').value.trim();
  inv.number = $('#invNumber').value.trim();
  inv.date = $('#invDate').value;
  inv.dueDate = $('#invDueDate').value || defaultDueDate(inv.date, inv.supplier);
  inv.total = parseFrNumber($('#invTotal').value);
  inv.vat = parseFrNumber($('#invVat').value);
  inv.totalTtc = parseFrNumber($('#invTotalTtc').value);
  inv.paymentMode = $('#invPaymentMode').value;

  if (!inv.supplier) { toast('Indique le fournisseur', 'warn'); return; }
  if (!inv.lines.length) { toast('Aucune ligne à enregistrer', 'warn'); return; }

  // Auto-création du fournisseur s'il n'existe pas
  const supExists = Object.values(STATE.suppliers).some(s => normalize(s.name) === normalize(inv.supplier));
  if (!supExists) {
    const sid = uid();
    STATE.suppliers[sid] = { name: inv.supplier, payment: 30, discount: 0 };
    await fbWrite('suppliers/' + sid, STATE.suppliers[sid]);
  }

  // Détection des articles non référencés pour validation
  const newOnes = [];
  inv.lines.forEach((l, i) => {
    const m = findArticleMatch(l, inv.supplier);
    if (!m && l.code && l.designation) newOnes.push({ idx: i, line: l });
  });

  // Détermination du statut initial
  const stats = invoiceStats({ supplier: inv.supplier, lines: inv.lines });
  const initialStatus = (stats.danger > 0 || stats.warn > 0) ? 'to_check' : 'validated';

  // Sauvegarde
  const id = uid();
  const data = {
    supplier: inv.supplier,
    number: inv.number,
    date: inv.date,
    dueDate: inv.dueDate || null,
    total: isNaN(inv.total) ? null : inv.total,
    vat: isNaN(inv.vat) ? null : inv.vat,
    totalTtc: isNaN(inv.totalTtc) ? null : inv.totalTtc,
    paymentMode: inv.paymentMode || 'virement',
    status: initialStatus,
    payments: [],
    lines: inv.lines.map(l => ({
      code: l.code || '',
      designation: l.designation || '',
      qty: parseFloat(l.qty) || 1,
      unit: l.unit || 'U',
      pu: parseFloat(l.pu) || 0,
      total: (parseFloat(l.qty) || 1) * (parseFloat(l.pu) || 0),
    })),
    createdAt: Date.now(),
  };
  STATE.invoices[id] = data;
  await fbWrite('invoices/' + id, data);

  toast('Facture enregistrée', 'ok');

  if (newOnes.length) {
    STATE.pendingArticles = newOnes.map(n => ({ ...n, supplier: inv.supplier }));
    openValidationModal();
  } else {
    resetCurrentInvoice();
  }

  renderAll();
}

// ============================================================
// MODAL VALIDATION ARTICLES NOUVEAUX
// ============================================================

function openValidationModal() {
  const list = $('#validationList');
  list.innerHTML = STATE.pendingArticles.map((p, i) => `
    <div class="validation-item">
      <input type="checkbox" id="val_${i}" checked>
      <div class="info">
        <strong>${escapeHtml(p.line.code || '—')} — ${escapeHtml(p.line.designation || '')}</strong>
        <span>Fournisseur : ${escapeHtml(p.supplier)} · PU facturé : ${fmtMoney(p.line.pu)}</span>
      </div>
      <input type="number" step="0.0001" class="price-input" id="valprice_${i}" value="${p.line.pu}" title="Prix de référence">
    </div>
  `).join('');
  $('#validationModal').hidden = false;
}

async function validateNewArticles() {
  for (let i = 0; i < STATE.pendingArticles.length; i++) {
    const cb = $('#val_' + i); if (!cb || !cb.checked) continue;
    const price = parseFrNumber($('#valprice_' + i).value);
    const p = STATE.pendingArticles[i];
    const id = uid();
    const a = {
      code: p.line.code || '',
      designation: p.line.designation || '',
      supplier: p.supplier,
      category: 'autre',
      unit: p.line.unit || 'U',
      price,
      tolerance: null,
      notes: 'Créé depuis facture',
      updatedAt: Date.now(),
    };
    STATE.articles[id] = a;
    await fbWrite('articles/' + id, a);
  }
  $('#validationModal').hidden = true;
  STATE.pendingArticles = [];
  toast('Articles ajoutés à la base', 'ok');
  renderArticles(); resetCurrentInvoice();
}

// ============================================================
// IMPORT / EXPORT
// ============================================================

function importArticlesXlsx(file) {
  const reader = new FileReader();
  reader.onload = async e => {
    try {
      const wb = XLSX.read(e.target.result, { type: 'array' });
      const sheet = wb.Sheets[wb.SheetNames[0]];
      const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
      let added = 0;
      for (const r of rows) {
        const a = {
          code: String(r.code || r.Code || r.CODE || r.reference || r['Référence'] || '').trim(),
          designation: String(r.designation || r.Designation || r.DESIGNATION || r['Désignation'] || r.libelle || r.Libellé || '').trim(),
          supplier: String(r.supplier || r.fournisseur || r.Fournisseur || r.FOURNISSEUR || '').trim(),
          category: String(r.category || r.categorie || r['Catégorie'] || 'autre').toLowerCase().trim(),
          unit: String(r.unit || r.unite || r['Unité'] || 'U').trim(),
          price: parseFrNumber(r.price || r.prix || r.Prix || r.PRIX || r.PU || r['Prix unitaire'] || 0),
          tolerance: r.tolerance || r.Tolerance || r['Tolérance'] || null,
          supplierCode: String(r.supplierCode || r['Code fournisseur'] || '').trim(),
          notes: String(r.notes || r.Notes || '').trim(),
          updatedAt: Date.now(),
        };
        if (!a.designation || isNaN(a.price)) continue;
        if (a.tolerance === '' || a.tolerance == null) a.tolerance = null;
        else a.tolerance = parseFrNumber(a.tolerance);
        const id = uid();
        STATE.articles[id] = a;
        await fbWrite('articles/' + id, a);
        added++;
      }
      toast(`${added} article(s) importé(s)`, 'ok');
      renderArticles();
    } catch (err) {
      console.error(err); toast('Erreur import : ' + err.message, 'danger');
    }
  };
  reader.readAsArrayBuffer(file);
}

function exportArticlesXlsx() {
  const rows = Object.values(STATE.articles).map(a => ({
    Code: a.code, Désignation: a.designation, Fournisseur: a.supplier,
    Catégorie: a.category, Unité: a.unit, 'Prix HT': a.price,
    Tolérance: a.tolerance, 'Code fournisseur': a.supplierCode,
    Notes: a.notes, 'Mise à jour': a.updatedAt ? new Date(a.updatedAt).toLocaleString('fr-FR') : '',
  }));
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Articles');
  XLSX.writeFile(wb, `articles_${today()}.xlsx`);
}

function exportInvoicesXlsx() {
  const rows = [];
  Object.values(STATE.invoices).forEach(inv => {
    (inv.lines || []).forEach(l => {
      const m = findArticleMatch(l, inv.supplier);
      const e = computeEcart(l, m ? m.article : null);
      rows.push({
        Date: inv.date, Fournisseur: inv.supplier, 'N° facture': inv.number,
        Code: l.code, Désignation: l.designation, Qté: l.qty, Unité: l.unit,
        'PU facturé': l.pu, 'PU référence': m ? m.article.price : '',
        'Écart %': e.pct == null ? '' : Number(e.pct.toFixed(2)),
        Statut: e.level, Surcoût: e.overcost > 0 ? Number(e.overcost.toFixed(2)) : 0,
      });
    });
  });
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Factures');
  XLSX.writeFile(wb, `factures_${today()}.xlsx`);
}

function exportEcartsXlsx() {
  const rows = [];
  Object.values(STATE.invoices).forEach(inv => {
    (inv.lines || []).forEach(l => {
      const m = findArticleMatch(l, inv.supplier);
      const e = computeEcart(l, m ? m.article : null);
      if (e.level === 'warn' || e.level === 'danger') {
        rows.push({
          Date: inv.date, Fournisseur: inv.supplier, 'N° facture': inv.number,
          Code: l.code, Désignation: l.designation, Qté: l.qty,
          'PU facturé': l.pu, 'PU référence': m.article.price,
          'Écart %': Number(e.pct.toFixed(2)), Niveau: e.level,
          Surcoût: Number(e.overcost.toFixed(2)),
        });
      }
    });
  });
  if (!rows.length) { toast('Aucun écart à exporter', 'warn'); return; }
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Écarts');
  XLSX.writeFile(wb, `ecarts_${today()}.xlsx`);
}

function backupJson() {
  const data = {
    version: 1,
    exportedAt: new Date().toISOString(),
    articles: STATE.articles,
    suppliers: STATE.suppliers,
    invoices: STATE.invoices,
    config: STATE.config,
  };
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = `facturecheck_backup_${today()}.json`;
  a.click(); URL.revokeObjectURL(url);
}

function restoreJson(file) {
  const reader = new FileReader();
  reader.onload = async e => {
    if (!confirm('Cela remplacera toutes les données actuelles. Continuer ?')) return;
    try {
      const d = JSON.parse(e.target.result);
      STATE.articles = d.articles || {};
      STATE.suppliers = d.suppliers || {};
      STATE.invoices = d.invoices || {};
      if (d.config) STATE.config = { ...STATE.config, ...d.config };
      if (STATE.fb.connected) {
        await STATE.fb.db.ref('facturecheck').set({
          articles: STATE.articles,
          suppliers: STATE.suppliers,
          invoices: STATE.invoices,
        });
      }
      saveLocal();
      toast('Sauvegarde restaurée', 'ok');
      renderAll();
    } catch (err) {
      toast('Fichier invalide', 'danger');
    }
  };
  reader.readAsText(file);
}

async function resetAll() {
  if (!confirm('⚠️ Supprimer TOUTES les données ? Cette action est irréversible.')) return;
  if (!confirm('Vraiment sûr ? Articles, fournisseurs et factures seront effacés.')) return;
  STATE.articles = {}; STATE.suppliers = {}; STATE.invoices = {};
  if (STATE.fb.connected) {
    try { await STATE.fb.db.ref('facturecheck').remove(); } catch(e) {}
  }
  saveLocal();
  toast('Données réinitialisées', 'ok');
  renderAll();
}

// ============================================================
// HELPERS
// ============================================================

function escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
}
function escapeAttr(s) { return escapeHtml(s); }

function renderAll() {
  renderArticles(); renderSuppliers(); renderSuppliersDatalists();
  renderInvoices(); renderEcarts(); renderKPI();
  renderPayment(); renderAccounts();
}

// ============================================================
// INIT & EVENT LISTENERS
// ============================================================

function bindEvents() {
  // Tabs
  renderTabs();

  // Dropzone
  const dz = $('#dropzone'), input = $('#fileInput');
  dz.addEventListener('click', e => { if (e.target.tagName !== 'BUTTON') input.click(); });
  $('#btnPick').addEventListener('click', () => input.click());
  input.addEventListener('change', () => handleFiles(input.files));
  ['dragenter','dragover'].forEach(ev =>
    dz.addEventListener(ev, e => { e.preventDefault(); dz.classList.add('dragover'); }));
  ['dragleave','drop'].forEach(ev =>
    dz.addEventListener(ev, e => { e.preventDefault(); dz.classList.remove('dragover'); }));
  dz.addEventListener('drop', e => handleFiles(e.dataTransfer.files));

  // Recommencer
  $('#btnRescan').addEventListener('click', resetCurrentInvoice);
  $('#btnCancelInvoice').addEventListener('click', resetCurrentInvoice);
  $('#btnSaveInvoice').addEventListener('click', saveCurrentInvoice);
  $('#btnAddLine').addEventListener('click', () => {
    if (!STATE.currentInvoice) STATE.currentInvoice = { lines: [] };
    STATE.currentInvoice.lines.push({ code: '', designation: '', qty: 1, unit: 'U', pu: 0, total: 0 });
    renderCurrentLines();
  });
  ['invSupplier','invNumber','invDate','invTotal','invDueDate','invVat','invTotalTtc','invPaymentMode'].forEach(id =>
    $('#'+id).addEventListener('change', () => {
      // Auto-recalc TVA/TTC quand on change HT, TVA ou TTC
      if (id === 'invTotal' || id === 'invVat' || id === 'invTotalTtc') recalcInvoiceTotals();
      // Auto-échéance quand on change la date ou le fournisseur
      if (id === 'invDate' || id === 'invSupplier') {
        if (!$('#invDueDate').value) {
          $('#invDueDate').value = defaultDueDate($('#invDate').value, $('#invSupplier').value);
        }
      }
      renderCurrentLines();
    }));

  // Articles
  $('#btnNewArticle').addEventListener('click', () => openArticleModal());
  $('#btnSaveArticle').addEventListener('click', saveArticle);
  $('#btnDeleteArticle').addEventListener('click', deleteArticle);
  $('#searchArticles').addEventListener('input', renderArticles);
  $('#filterSupplier').addEventListener('change', renderArticles);
  $('#filterCategory').addEventListener('change', renderArticles);
  $('#btnImportArticles').addEventListener('click', () => {
    const inp = document.createElement('input');
    inp.type = 'file'; inp.accept = '.xlsx,.xls,.csv';
    inp.onchange = () => inp.files[0] && importArticlesXlsx(inp.files[0]);
    inp.click();
  });
  $('#btnExportArticles').addEventListener('click', exportArticlesXlsx);

  // Suppliers
  $('#btnNewSupplier').addEventListener('click', () => openSupplierModal());
  $('#btnSaveSupplier').addEventListener('click', saveSupplier);
  $('#btnDeleteSupplier').addEventListener('click', deleteSupplier);

  // Invoices
  $('#searchInvoices').addEventListener('input', renderInvoices);
  $('#histSupplier').addEventListener('change', renderInvoices);
  $('#histMonth').addEventListener('change', renderInvoices);
  $('#btnExportInvoices').addEventListener('click', exportInvoicesXlsx);
  $('#btnDeleteInvoice').addEventListener('click', deleteInvoice);

  // Écarts
  $('#ecartPeriod').addEventListener('change', renderEcarts);
  $('#ecartSupplier').addEventListener('change', renderEcarts);
  $('#ecartLevel').addEventListener('change', renderEcarts);
  $('#btnExportEcarts').addEventListener('click', exportEcartsXlsx);

  // Suivi facturier
  $('#searchPayment').addEventListener('input', renderPayment);
  $('#paymentStatus').addEventListener('change', renderPayment);
  $('#paymentSupplier').addEventListener('change', renderPayment);
  $('#paymentDue').addEventListener('change', renderPayment);
  $('#btnExportPayment').addEventListener('click', exportPaymentXlsx);
  $('#btnSavePayment').addEventListener('click', savePayment);

  // Comptes fournisseurs
  $('#searchAccountSup').addEventListener('input', renderAccounts);
  $('#btnExportAccounts').addEventListener('click', exportAccountsXlsx);

  // Validation
  $('#btnValidateNew').addEventListener('click', validateNewArticles);

  // Modals close
  $$('[data-close]').forEach(b =>
    b.addEventListener('click', () => $('#' + b.dataset.close).hidden = true));

  // Settings
  $('#cfgApiKey').value = STATE.config.firebase?.apiKey || '';
  $('#cfgAuthDomain').value = STATE.config.firebase?.authDomain || '';
  $('#cfgDbUrl').value = STATE.config.firebase?.databaseURL || '';
  $('#cfgProjectId').value = STATE.config.firebase?.projectId || '';
  $('#cfgThreshWarn').value = STATE.config.threshWarn;
  $('#cfgThreshDanger').value = STATE.config.threshDanger;

  $('#btnConnectFb').addEventListener('click', async () => {
    const cfg = {
      apiKey: $('#cfgApiKey').value.trim(),
      authDomain: $('#cfgAuthDomain').value.trim(),
      databaseURL: $('#cfgDbUrl').value.trim(),
      projectId: $('#cfgProjectId').value.trim(),
    };
    if (!cfg.apiKey || !cfg.databaseURL) { toast('API Key et Database URL requis', 'warn'); return; }
    STATE.config.firebase = cfg;
    saveLocal();
    const ok = await fbInit(cfg);
    if (ok) { toast('Firebase connecté', 'ok'); renderAll(); $('#fbInfo').textContent = 'Connecté à ' + cfg.projectId; }
    else toast('Connexion échouée — vérifie la config', 'danger');
  });
  $('#btnDisconnectFb').addEventListener('click', () => {
    fbDisconnect(); STATE.config.firebase = null; saveLocal();
    toast('Firebase déconnecté', 'ok'); $('#fbInfo').textContent = '';
  });
  $('#btnSaveThresh').addEventListener('click', () => {
    STATE.config.threshWarn = parseFloat($('#cfgThreshWarn').value) || 2;
    STATE.config.threshDanger = parseFloat($('#cfgThreshDanger').value) || 5;
    saveLocal(); toast('Seuils enregistrés', 'ok'); renderAll();
  });
  $('#btnBackup').addEventListener('click', backupJson);
  $('#btnRestore').addEventListener('click', () => $('#restoreInput').click());
  $('#restoreInput').addEventListener('change', e => e.target.files[0] && restoreJson(e.target.files[0]));
  $('#btnReset').addEventListener('click', resetAll);

  // Click outside modal
  $$('.modal').forEach(m =>
    m.addEventListener('click', e => { if (e.target === m) m.hidden = true; }));
}

async function init() {
  loadLocal();
  bindEvents();
  resetCurrentInvoice();

  if (STATE.config.firebase?.apiKey) {
    const ok = await fbInit(STATE.config.firebase);
    if (ok) $('#fbInfo').textContent = 'Connecté à ' + STATE.config.firebase.projectId;
  }
  renderAll();
}

document.addEventListener('DOMContentLoaded', init);
