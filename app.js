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
  { re: /cedeo|c[eé]d[ée]o/i, name: 'Cedeo' },
  { re: /brossette/i, name: 'Brossette' },
  { re: /rexel/i, name: 'Rexel' },
  { re: /w[uü]rth/i, name: 'Würth' },
  { re: /point\s*p|point[\s.-]*p\b/i, name: 'Point P' },
  { re: /saint[\s-]*gobain/i, name: 'Saint-Gobain' },
  { re: /sonepar/i, name: 'Sonepar' },
  { re: /yesss\s*electrique/i, name: 'Yesss Électrique' },
  { re: /castorama/i, name: 'Castorama' },
  { re: /leroy\s*merlin/i, name: 'Leroy Merlin' },
  { re: /prolians/i, name: 'Prolians' },
  { re: /tereva/i, name: 'Tereva' },
  { re: /frans\s*bonhomme/i, name: 'Frans Bonhomme' },
  { re: /richardson/i, name: 'Richardson' },
];

function parseInvoice(text) {
  const result = {
    supplier: '', number: '', date: '', dueDate: '',
    total: null, vat: null, totalTtc: null,
    paymentMode: 'virement', lines: [],
  };

  // 1. Fournisseur
  for (const p of SUPPLIER_PATTERNS) {
    if (p.re.test(text)) { result.supplier = p.name; break; }
  }
  if (!result.supplier) {
    for (const sid in STATE.suppliers) {
      const s = STATE.suppliers[sid];
      const re = new RegExp(s.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
      if (re.test(text)) { result.supplier = s.name; break; }
    }
  }

  // 2. N° de facture
  const numMatches = [
    /facture\s*(?:n[°o]|num(?:[ée]ro)?)?\s*[:#]?\s*([A-Z0-9][A-Z0-9_\-\/]{3,20})/i,
    /n[°o]\s*facture\s*[:#]?\s*([A-Z0-9][A-Z0-9_\-\/]{3,20})/i,
    /invoice\s*(?:no|number|#)?\s*[:#]?\s*([A-Z0-9][A-Z0-9_\-\/]{3,20})/i,
    /\bFA[\s_\-]*[\dA-Z][\dA-Z_\-\/]{4,15}/i,
  ];
  for (const re of numMatches) {
    const m = text.match(re);
    if (m) { result.number = (m[1] || m[0]).trim().replace(/\s+/g, ''); break; }
  }

  // 3. Date facture
  const dateMatches = [
    /date\s*(?:de\s*)?(?:facture|facturation|emission)?\s*[:.]?\s*(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})/i,
    /\b(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})\b/,
  ];
  for (const re of dateMatches) {
    const m = text.match(re);
    if (m) {
      const parts = m[1].split(/[\/\-\.]/);
      if (parts.length === 3) {
        let [d, mo, y] = parts;
        if (y.length === 2) y = (parseInt(y) > 50 ? '19' : '20') + y;
        if (parseInt(d) > 31) [d, y] = [y, d];
        result.date = `${y}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
        break;
      }
    }
  }

  // 4. Échéance (date d'échéance / à payer avant / etc.)
  const dueMatches = [
    /(?:date\s*(?:d[''])?\s*[ée]ch[ée]ance|[ée]ch[ée]ance|[àa]\s*payer\s*(?:avant|le)|payable\s*le)\s*[:.]?\s*(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})/i,
  ];
  for (const re of dueMatches) {
    const m = text.match(re);
    if (m) {
      const parts = m[1].split(/[\/\-\.]/);
      if (parts.length === 3) {
        let [d, mo, y] = parts;
        if (y.length === 2) y = '20' + y;
        if (parseInt(d) > 31) [d, y] = [y, d];
        result.dueDate = `${y}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
        break;
      }
    }
  }

  // 5. Total HT
  const totalMatches = [
    /total\s*(?:net\s*)?h\.?t\.?\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
    /montant\s*h\.?t\.?\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
    /net\s*[àa]\s*payer\s*h\.?t\.?\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
    /total\s*hors\s*taxes?\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
  ];
  for (const re of totalMatches) {
    const m = text.match(re);
    if (m) { const v = parseFrNumber(m[1]); if (!isNaN(v)) { result.total = v; break; } }
  }

  // 6. TVA
  const vatMatches = [
    /(?:total\s*)?(?:montant\s*)?t\.?v\.?a\.?\s*(?:\d+[\s,\.]*\d*\s*%)?\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
    /(?:total\s*)?taxes?\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
  ];
  for (const re of vatMatches) {
    const m = text.match(re);
    if (m) { const v = parseFrNumber(m[1]); if (!isNaN(v) && v > 0 && v < 50000) { result.vat = v; break; } }
  }

  // 7. Total TTC
  const ttcMatches = [
    /total\s*t\.?t\.?c\.?\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
    /net\s*[àa]\s*payer\s*(?:t\.?t\.?c\.?|en\s*euros)?\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
    /montant\s*t\.?t\.?c\.?\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
    /total\s*toutes\s*taxes\s*comprises\s*[:.]?\s*([\d\s\.,]+)\s*€?/i,
  ];
  for (const re of ttcMatches) {
    const m = text.match(re);
    if (m) { const v = parseFrNumber(m[1]); if (!isNaN(v)) { result.totalTtc = v; break; } }
  }

  // 8. Si on a HT mais pas TTC, on calcule à 20% (et inverse)
  if (result.total != null && result.totalTtc == null) {
    if (result.vat != null) result.totalTtc = result.total + result.vat;
    else result.totalTtc = +(result.total * 1.20).toFixed(2);
  } else if (result.total == null && result.totalTtc != null) {
    result.total = +(result.totalTtc / 1.20).toFixed(2);
    if (result.vat == null) result.vat = +(result.totalTtc - result.total).toFixed(2);
  }
  if (result.total != null && result.totalTtc != null && result.vat == null) {
    result.vat = +(result.totalTtc - result.total).toFixed(2);
  }

  // 9. Lignes
  result.lines = parseInvoiceLines(text);

  return result;
}

/**
 * Extrait les lignes article d'un texte de facture.
 * Format générique attendu par ligne (souplesse nécessaire) :
 *   [CODE] DESIGNATION ... QTE ... PU ... TOTAL
 * Stratégie : on cherche les lignes qui se terminent par 2 ou 3 nombres décimaux,
 * dont le dernier est plausiblement = qté * pu (à 5% près).
 */
function parseInvoiceLines(text) {
  const lines = [];
  const rawLines = text.split(/\r?\n/).map(l => l.trim()).filter(Boolean);

  for (const raw of rawLines) {
    // ignorer lignes qui contiennent des mots-clés totaux/entêtes (en début OU avec keywords forts au milieu)
    if (/^(total|tva|h\.?t\.?|t\.?t\.?c\.?|sous-total|net\s*[àa]\s*payer|montant|remise|escompte|page|date|n[°o]|client|adresse|tel|fax|email|siret|tva\s*intra)/i.test(raw)) continue;
    // ignorer aussi si la ligne contient un mot-clé de facture/document (évite "Facture N° FA-2026...")
    if (/\b(facture|invoice|bon\s*de\s*commande|bon\s*de\s*livraison|devis)\s*(n[°o]|num|#|:)/i.test(raw)) continue;
    if (/\b(total|sous[\s-]*total|montant\s+(ht|ttc)|net\s+[àa]\s+payer|tva\s+\d)/i.test(raw)) continue;
    if (raw.length < 8) continue;

    // chercher 2-3 nombres en fin de ligne
    const numRe = /([\d]+(?:[.,\s][\d]+)*(?:[.,][\d]{1,4})?)/g;
    const allNums = [];
    let m;
    while ((m = numRe.exec(raw)) !== null) {
      allNums.push({ str: m[1], idx: m.index, len: m[0].length });
    }
    if (allNums.length < 2) continue;

    // Le dernier nombre = total ligne ; avant-dernier = PU ; avant si présent = qté
    const lastN = allNums[allNums.length - 1];
    const totalLine = parseFrNumber(lastN.str);
    if (isNaN(totalLine) || totalLine <= 0 || totalLine > 50000) continue;

    const puN = allNums[allNums.length - 2];
    const pu = parseFrNumber(puN.str);
    if (isNaN(pu) || pu <= 0 || pu > 10000) continue;

    let qty = 1;
    let qtyN = null;
    if (allNums.length >= 3) {
      qtyN = allNums[allNums.length - 3];
      const q = parseFrNumber(qtyN.str);
      if (!isNaN(q) && q > 0 && q < 10000) {
        // vérifier cohérence : qty * pu ~= totalLine (à 5% près)
        const expected = q * pu;
        if (Math.abs(expected - totalLine) / totalLine < 0.06) {
          qty = q;
        } else {
          qtyN = null; // on garde qty=1
        }
      } else {
        qtyN = null;
      }
    }

    // partie à gauche du premier nombre utilisé = code + désignation
    const cutIdx = qtyN ? qtyN.idx : puN.idx;
    let leftPart = raw.slice(0, cutIdx).trim();
    if (!leftPart || leftPart.length < 3) continue;

    // tenter de séparer code et désignation : code = premier token alphanumérique
    let code = '';
    let desig = leftPart;
    const codeMatch = leftPart.match(/^([A-Z0-9][A-Z0-9_\-\.\/]{2,18})\s+(.+)$/i);
    if (codeMatch) { code = codeMatch[1]; desig = codeMatch[2]; }

    desig = desig.replace(/\s+/g, ' ').trim();
    if (desig.length > 100) desig = desig.slice(0, 100);

    lines.push({
      code,
      designation: desig,
      qty,
      unit: 'U',
      pu,
      total: totalLine,
    });
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
