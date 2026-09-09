/**
 * JudiQ AI — Interactive Cross-Examination Risk Simulator & Legal Strategy Workbench
 * Dynamic courtroom simulation sandbox for testing defense strategies and analyzing survivability in real-time.
 */

export class JudiQStrategySimulator {
    constructor() {
        this.currentPreset = 's138_signature';
        this.init();
    }

    init() {
        window.selectSimulatorPreset = (presetKey) => this.loadPreset(presetKey);
        window.recalculateSimulatorScore = () => this.calculateScore();
    }

    loadPreset(presetKey) {
        this.currentPreset = presetKey;
        const presets = {
            s138_signature: {
                title: "Cheque Bounce (Sec 138) — Disputed Signature & Stop Payment",
                domain: "NI Act Section 138",
                noticeDelay: 12,
                debtExisted: true,
                securityCheque: false,
                signatureDisputed: true,
                evidence65B: true,
                baseScore: 68,
                attackVector: "Opposing counsel will demand forensic handwriting expert opinion under Sec. 45 Evidence Act and call the bank manager to verify specimen cards.",
                counterStrategy: "File application for comparison of signatures by State Forensic Science Laboratory; establish lack of authorization register.",
                ratio: "Linny D’Souza v. Vijay Kumar (2019) — Difference in signature requires strict proof of drawing under Sec. 138."
            },
            s138_notice_delay: {
                title: "Cheque Bounce (Sec 138) — Notice Delay > 30 Days",
                domain: "NI Act Section 138",
                noticeDelay: 42,
                debtExisted: true,
                securityCheque: false,
                signatureDisputed: false,
                evidence65B: true,
                baseScore: 25,
                attackVector: "Fatal limitation defect! Demand notice dispatched on Day 42, exceeding statutory 30-day window under Sec. 138(b).",
                counterStrategy: "Move application under Sec. 142(1)(b) proviso seeking condonation of delay with affidavit explaining sufficient cause.",
                ratio: "Prem Chand Vijay Kumar v. Yashpal Singh (2005) — Cause of action arises only once statutory notice condition is met strictly."
            },
            s138_security_cheque: {
                title: "Cheque Bounce (Sec 138) — Security Cheque & Blank Signed Instrument",
                domain: "NI Act Section 138",
                noticeDelay: 10,
                debtExisted: false,
                securityCheque: true,
                signatureDisputed: false,
                evidence65B: true,
                baseScore: 48,
                attackVector: "Accused claims cheque was handed over as collateral/security without any existing legally enforceable debt.",
                counterStrategy: "Invoke statutory presumption under Section 139 & 118(a) NI Act; prove consideration passed via promissory note or ledger statement.",
                ratio: "Bir Singh v. Mukesh Kumar (2019) 4 SCC 197 — Even a blank signed cheque attracts Section 139 presumption once execution is admitted."
            },
            s138_electronic_records: {
                title: "Cheque Bounce (Sec 138) — Electronic Ledger & Missing S.65B Certificate",
                domain: "NI Act Section 138",
                noticeDelay: 8,
                debtExisted: true,
                securityCheque: false,
                signatureDisputed: false,
                evidence65B: false,
                baseScore: 42,
                attackVector: "WhatsApp confirmation chats, email demands, and computerized bank statements submitted without mandatory Section 65B Certificate / BSA Section 63.",
                counterStrategy: "Produce Section 65B / BSA Section 63 certificate by the system administrator during trial to cure evidentiary inadmissibility.",
                ratio: "Arjun Panditrao Khotkar v. Kailash Kushanrao Gorantyal (2020) — S.65B Certificate is an indispensable condition precedent for secondary electronic records."
            }
        };

        const config = presets[presetKey] || presets.s138_signature;
        
        // Update DOM elements if present
        const titleEl = document.getElementById('simTitle');
        const attackEl = document.getElementById('simAttackVector');
        const counterEl = document.getElementById('simCounterStrategy');
        const ratioEl = document.getElementById('simRatio');
        const domainBadge = document.getElementById('simDomainBadge');

        if (titleEl) titleEl.textContent = config.title;
        if (attackEl) attackEl.textContent = config.attackVector;
        if (counterEl) counterEl.textContent = config.counterStrategy;
        if (ratioEl) ratioEl.textContent = config.ratio;
        if (domainBadge) domainBadge.textContent = config.domain;

        // Update form toggles
        const noticeInput = document.getElementById('simNoticeDelayInput');
        const sigCheck = document.getElementById('simSignatureCheck');
        const secCheck = document.getElementById('simSecurityCheck');
        const e65bCheck = document.getElementById('sim65BCheck');

        if (noticeInput) noticeInput.value = config.noticeDelay;
        if (sigCheck) sigCheck.checked = config.signatureDisputed;
        if (secCheck) secCheck.checked = config.securityCheque;
        if (e65bCheck) e65bCheck.checked = config.evidence65B;

        // Highlight active preset button
        document.querySelectorAll('.sim-preset-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.preset === presetKey);
        });

        this.calculateScore();
    }

    calculateScore() {
        const noticeDelay = parseInt(document.getElementById('simNoticeDelayInput')?.value || '15', 10);
        const sigDisputed = document.getElementById('simSignatureCheck')?.checked || false;
        const securityCheque = document.getElementById('simSecurityCheck')?.checked || false;
        const e65bPresent = document.getElementById('sim65BCheck')?.checked || false;

        let score = 90;

        if (noticeDelay > 30) {
            score -= 55; // Fatal limitation defect
        } else if (noticeDelay > 25) {
            score -= 10;
        }

        if (sigDisputed) score -= 20;
        if (securityCheque) score -= 15;
        if (!e65bPresent) score -= 25;

        score = Math.max(10, Math.min(99, score));

        // Update Score Gauge
        const scoreMeter = document.getElementById('simScoreMeter');
        const scoreVal = document.getElementById('simScoreValue');
        const scoreStatus = document.getElementById('simScoreStatus');

        if (scoreMeter) {
            scoreMeter.style.width = `${score}%`;
            if (score >= 75) {
                scoreMeter.style.background = 'linear-gradient(90deg, #10b981, #059669)';
            } else if (score >= 50) {
                scoreMeter.style.background = 'linear-gradient(90deg, #f59e0b, #d97706)';
            } else {
                scoreMeter.style.background = 'linear-gradient(90deg, #ef4444, #dc2626)';
            }
        }

        if (scoreVal) scoreVal.textContent = `${score}%`;
        if (scoreStatus) {
            if (score >= 75) {
                scoreStatus.textContent = "High Courtroom Survivability";
                scoreStatus.className = "sim-score-badge safe";
            } else if (score >= 50) {
                scoreStatus.textContent = "Moderate Risk — Defense Counter Required";
                scoreStatus.className = "sim-score-badge warning";
            } else {
                scoreStatus.textContent = "Fatal Procedural Vulnerability Detected";
                scoreStatus.className = "sim-score-badge danger";
            }
        }
    }
}
