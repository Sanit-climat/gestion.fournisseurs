# FactureCheck — Sanit Climat

Application web standalone de **scan de factures fournisseurs**, **vérification automatique des prix** et **suivi facturier complet** (statuts, paiements, balance âgée par fournisseur).

## Fonctionnalités

### Scan & vérification des prix
- 📄 **Scan multi-format** : PDF (texte ou scanné), PNG, JPG, WEBP — extraction auto via PDF.js + Tesseract.js OCR
- 🏷️ **Auto-détection fournisseur** : Cedeo, Brossette, Rexel, Würth, Point P, Saint-Gobain, Sonepar, Yesss Électrique, Castorama, Leroy Merlin, Prolians, Tereva, Frans Bonhomme, Richardson (extensible)
- 📊 **Base articles fournisseurs** : code, désignation, fournisseur, catégorie, unité, prix de référence, tolérance personnalisée
- ✅ **Détection d'écarts en temps réel** : 3 niveaux (vert ≤ tolérance, orange 2-5%, rouge > 5%)
- 💰 **Calcul du surcoût** : (PU facturé − PU référence) × quantité

### Suivi facturier
- 🏷️ **5 statuts** : `À vérifier` (auto si écarts détectés), `Validée`, `Payée partiellement`, `Soldée`, `Litige`
- 💳 **Paiements partiels** avec historique : date, montant, mode (virement, prélèvement, chèque, CB, espèces, LCR, autre), référence, notes
- 📋 **Gestion des avoirs et escomptes** comme types de paiement spécifiques (réduisent le solde dû)
- 🧮 **Calcul automatique TVA/TTC** : si HT et TTC saisis → TVA déduite ; si TTC seul → HT estimé à 20%
- 📅 **Échéance auto** calculée depuis la date facture + délai de paiement du fournisseur
- 🚦 **Statut auto** mis à jour selon les paiements (sauf si en litige ou à vérifier)

### Comptes fournisseurs
- 💼 **Compte par fournisseur** type "logiciel comptable simplifié" (sans écritures comptables)
- 📊 **Solde courant** affiché dans la sidebar avec tri par montant dû
- ⏱️ **Balance âgée 30/60/90j** : Non échu / 0-30 j / 31-60 j / 61-90 j / +90 j (couleurs progressives)
- 📋 **Liste de toutes les factures** par fournisseur avec statut, montants, paiements
- 🔴 **Détection automatique des retards** avec marquage visuel rouge

### Tableaux de bord
- 📈 **KPIs Suivi facturier** : Total dû · À payer cette semaine · En retard · À vérifier · Soldé ce mois
- 📈 **KPIs Comptes fournisseurs** : Solde global · Non échu · 0-30j · 31-60j · +90j
- 📈 **KPIs Écarts prix** : Factures analysées · Conformes · Modérés · Critiques · Surcoût total

### Imports/Exports Excel
- Articles (import + export)
- Suivi facturier (export avec statuts, échéances, paiements, restes dûs)
- Balance fournisseurs (export avec balance âgée)
- Écarts prix (export rapport)
- Sauvegarde/restauration JSON complète

### Stockage
- 🔥 **Firebase Realtime Database** (région europe-west1) avec sync temps réel multi-postes
- 💾 **Fallback localStorage** si pas de Firebase configuré
- 🌙 **UI dark navy/orange** cohérente avec ton écosystème (Tarif, ChauffTrack, InterTrack)

## Architecture

```
facturecheck/
├── index.html              # Interface complète (8 onglets)
├── styles.css              # Theme dark navy/orange
├── app.js                  # Logique métier complète
├── articles_template.csv   # Modèle d'import Excel
└── README.md
```

**Aucun serveur** : tout tourne dans le navigateur. Données stockées dans Firebase Realtime Database et/ou localStorage.

## Onglets

1. **Scan factures** — drag & drop + extraction + édition des lignes + validation prix
2. **Base articles** — référentiel des prix avec import/export Excel
3. **Fournisseurs** — annuaire avec coordonnées et conditions
4. **Écarts & alertes** — vue des écarts de prix détectés
5. **Historique** — toutes les factures
6. **Suivi facturier** — pilotage paiements (statuts, échéances, restes dûs)
7. **Comptes fournisseurs** — solde courant + balance âgée + échéancier par fournisseur
8. **Réglages** — Firebase, seuils, sauvegarde/restauration

## Workflow d'utilisation

### Réception d'une facture
1. **Scan factures** → drag & drop le PDF
2. L'app extrait : fournisseur, n° facture, dates, total HT/TVA/TTC, mode de paiement, lignes article
3. L'échéance est calculée automatiquement (date + délai fournisseur)
4. Vérifier / corriger les lignes mal détectées
5. Cliquer **Enregistrer la facture**
6. Statut initial automatique : `À vérifier` si écarts détectés, sinon `Validée`

### Validation des écarts
1. **Suivi facturier** → ouvrir les factures `À vérifier`
2. Examiner les lignes en orange/rouge dans la modale
3. Soit modifier le statut en `Validée` (accepter), soit en `Litige` (contestation)

### Enregistrement d'un paiement
1. **Suivi facturier** ou **Comptes fournisseurs** → bouton `Régler` sur la facture
2. Saisir : type (paiement / avoir / escompte), date, montant (pré-rempli au reste dû), mode, référence
3. Le statut passe automatiquement à `Soldée` ou `Payée partiellement`

### Suivi mensuel d'un fournisseur
1. **Comptes fournisseurs** → cliquer sur le fournisseur dans la sidebar
2. Vue d'ensemble : solde dû, balance âgée 30/60/90j, échéancier
3. Liste de toutes les factures avec leur statut
4. Export Excel de la balance pour rapprochement comptable

## Configuration Firebase

1. Sur [console.firebase.google.com](https://console.firebase.google.com), crée un projet
2. Active **Realtime Database** en région `europe-west1`
3. Règles minimales :
   ```json
   {
     "rules": {
       "facturecheck": {
         ".read": true,
         ".write": true
       }
     }
   }
   ```
   ⚠️ Pour la production, ajouter Firebase Auth.
4. Dans **Project Settings → General → Your apps**, copie la config web
5. Dans l'app : **Réglages → Connexion Firebase** → coller et **Connecter**

## Déploiement GitHub Pages

```bash
git init
git add .
git commit -m "FactureCheck v1.1"
git remote add origin https://github.com/sanit-climat/facturecheck.git
git branch -M main
git push -u origin main
```

Active GitHub Pages dans **Settings → Pages → Branch: `main`**. L'app sera à `https://sanit-climat.github.io/facturecheck/`.

## Modèle d'import Excel articles

| Code | Désignation | Fournisseur | Catégorie | Unité | Prix HT | Tolérance | Code fournisseur | Notes |
|------|-------------|-------------|-----------|-------|---------|-----------|------------------|-------|
| CED-12345 | Coude cuivre 22 | Cedeo | plomberie | U | 2.45 | 3 | 12345 | |

## Données stockées

```
facturecheck/
├── articles/{id}    # Référentiel prix
├── suppliers/{id}   # Annuaire fournisseurs (avec délai paiement, remise)
└── invoices/{id}    # Factures avec :
                     #   - Données : fournisseur, n°, date, dueDate
                     #   - Montants : total HT/TVA/TTC, paymentMode
                     #   - Statut : to_check / validated / partial / paid / dispute
                     #   - Lignes : code, désignation, qté, unité, pu, total
                     #   - Paiements : id, type, date, amount, mode, reference, notes
```

## Réglages des seuils

Par défaut :
- **Vert (conforme)** : écart ≤ 2 %
- **Orange (modéré)** : écart entre 2 % et 5 %
- **Rouge (critique)** : écart > 5 %

Modifiable dans **Réglages → Seuils d'alerte**.

## Limites connues

- L'OCR est en français uniquement (modifiable dans `app.js`)
- Le parsing des lignes est heuristique, certaines factures atypiques peuvent nécessiter une correction manuelle
- Pas de gestion multi-utilisateurs avec permissions (à ajouter via Firebase Auth si besoin)
- Pas d'écritures comptables — c'est un outil opérationnel, pas un logiciel de comptabilité

## Licence

Application interne Sanit Climat SAS — non diffusable.

