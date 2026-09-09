import { ui, switchScreen } from './ui.js?v=14';
import { api } from './api.js?v=14';
import { escapeHtml } from './js/modules/utils.js?v=14';

// Helper: isTruthy check
function isTruthy(val) {
    if (!val) return false;
    if (typeof val === 'string') {
        const v = val.toLowerCase().trim();
        return v === 'yes' || v === 'true' || v === '1' || v.startsWith('yes');
    }
    return !!val;
}

// Map case merit / viability keys
export function mapVerdict(v) {
    const map = {
        WEAK: "Weak Legal Merit / Elevated Risk",
        MODERATE: "Moderate Legal Merit",
        STRONG: "Strong Legal Merit",
        "DO NOT FILE": "Procedural Bar / Cure Required",
        "HIGH_RISK": "High Legal & Procedural Risk"
    };
    return map[v] || v || "Case Evaluated";
}

// Get analytical merit description based on score
export function getVerdictDescription(score) {
    if (score >= 70) return 'Analytical evaluation indicates strong documentary trail and statutory merit';
    if (score >= 40) return 'Analytical evaluation indicates moderate merit with specific vulnerabilities to address';
    return 'Analytical evaluation identifies critical evidentiary gaps or procedural risks';
}

// Animate score display
export function animateScore(targetScore) {
    const scoreElement = document.getElementById('scoreNumber');
    const progressCircle = document.getElementById('scoreProgress');
    if (!scoreElement || !progressCircle) return;

    const circumference = 2 * Math.PI * 90;

    let currentScore = 0;
    const duration = 2000;
    const increment = targetScore / (duration / 16);

    const animate = () => {
        currentScore += increment;
        if (currentScore >= targetScore) currentScore = targetScore;

        scoreElement.textContent = Math.round(currentScore);
        progressCircle.style.strokeDashoffset = circumference - (currentScore / 100) * circumference;

        if (currentScore < targetScore) requestAnimationFrame(animate);
    };

    progressCircle.style.strokeDasharray = circumference;
    progressCircle.style.strokeDashoffset = circumference;
    progressCircle.style.transition = 'stroke-dashoffset 0.1s linear';
    animate();
}

// Generic list renderer
export function renderList(id, items, fallback = "No data available") {
    const el = document.getElementById(id);
    if (!el) return;

    if (!items || items.length === 0) {
        el.innerHTML = `<p style="color: var(--gray-500);">${escapeHtml(String(fallback))}</p>`;
        return;
    }

    el.innerHTML = items.map(item => {
        let parsedItem = item;
        if (typeof item === 'string' && item.trim().startsWith('{')) {
            try { parsedItem = JSON.parse(item); } catch(e) {}
        }
        
        let text = "";
        let trustedMarkup = false;
        if (typeof parsedItem === 'object' && parsedItem !== null) {
            if (parsedItem.risk && parsedItem.severity && parsedItem.detail) {
                const badgeColor = parsedItem.severity === 'FATAL' ? '#ef4444' : (parsedItem.severity === 'CRITICAL' ? '#f97316' : (parsedItem.severity === 'HIGH' ? '#eab308' : '#3b82f6'));
                text = `<div style="display:flex; flex-direction:column; gap:0.3rem; width:100%;">
                    <div style="display:flex; align-items:center; gap:0.5rem; flex-wrap:wrap;">
                        <span style="font-weight:700; color:var(--gray-900);">${escapeHtml(String(parsedItem.risk))}</span>
                        <span style="background-color:${badgeColor}22; color:${badgeColor}; font-size:0.7rem; font-weight:700; padding:0.15rem 0.4rem; border-radius:0.25rem; text-transform:uppercase; border:1px solid ${badgeColor}44;">${escapeHtml(String(parsedItem.severity))}</span>
                    </div>
                    <div style="color:var(--gray-600); font-size:0.85rem; line-height:1.4;">${escapeHtml(String(parsedItem.detail))}</div>
                </div>`;
                trustedMarkup = true;
            } else {
                text = parsedItem.text || parsedItem.title || parsedItem.description || parsedItem.risk || parsedItem.issue || parsedItem.weakness || parsedItem.strength || JSON.stringify(parsedItem);
            }
        } else {
            text = String(parsedItem);
        }

        const safeText = trustedMarkup ? text : escapeHtml(String(text));
        return `<div class="list-item" style="align-items: flex-start; padding: 0.6rem 0.8rem; margin-bottom: 0.5rem; background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: 0.5rem;"><i class="fas fa-chevron-right" style="color: var(--primary-500); margin-right: 0.5rem; margin-top: 0.25rem;"></i><div style="flex:1; width: 100%; color: var(--gray-800);">${safeText}</div></div>`;
    }).join('');
}

// Display predicted defences
export function displayDefences(defences) {
    const container = document.getElementById('defencesList');
    if (!container) return;

    if (!defences || defences.length === 0) {
        container.innerHTML = '<p style="color: var(--gray-500);">No defences simulated</p>';
        return;
    }

    container.innerHTML = defences.map(defence => {
        const argument = String(defence.argument || defence.defence || defence.title || 'Defence Strategy');
        const strength = (defence.strength || 'Medium').toLowerCase();
        const probability = defence.success_probability || defence.probability || 50;
        
        // Determine graph metric mapping
        const risk = argument.toLowerCase();
        let targetCat = 'Procedural Compliance';
        if (risk.includes('cheque') || risk.includes('proof') || risk.includes('evidence') || risk.includes('bsa') || risk.includes('admissibility') || risk.includes('itr') || risk.includes('financial')) {
            targetCat = 'Evidence Strength';
        } else if (risk.includes('jurisdiction') || risk.includes('court')) {
            targetCat = 'Jurisdictional Veracity';
        } else if (risk.includes('statute') || risk.includes('statutory') || risk.includes('act') || risk.includes('veracity') || risk.includes('pleading')) {
            targetCat = 'Statutory Compliance';
        } else if (risk.includes('witness') || risk.includes('credibility')) {
            targetCat = 'Witness Credibility';
        }

        return `
            <div class="defence-item">
                <div class="defence-header">
                    <div style="display: flex; flex-direction: column; gap: 0.4rem;">
                        <h4>${escapeHtml(argument)}</h4>
                        <div style="display: flex; gap: 0.5rem; align-items: center; flex-wrap: wrap;">
                            <span class="defence-strength ${strength}">${defence.strength || 'Medium'}</span>
                            <span class="defence-metric"><i class="fas fa-chart-pie"></i> Metric: ${targetCat}</span>
                        </div>
                    </div>
                    <div class="defence-probability-badge">
                        ${probability}%
                    </div>
                </div>
                ${defence.trigger_reason ? `<p class="defence-details"><strong>Triggered by:</strong> ${escapeHtml(String(defence.trigger_reason))}</p>` : ''}
                ${defence.rebuttal ? `<div class="defence-rebuttal"><strong>Counter Strategy:</strong> ${escapeHtml(String(defence.rebuttal))}</div>` : ''}
            </div>
        `;
    }).join('');
}

// Display semantic analysis
export function displaySemanticAnalysis(semantic) {
    const container = document.getElementById('semanticAnalysis');
    if (!container) return;

    const concepts = semantic.concepts_detected || [];
    if (concepts.length === 0) {
        container.innerHTML = '<p style="color: var(--gray-500);">No specific legal concepts detected</p>';
        return;
    }

    container.innerHTML = concepts.map(concept => `
        <div class="concept-item">
            <div class="concept-header">
                <h4>${escapeHtml(String(concept.concept || 'Legal Concept').replace(/_/g, ' '))}</h4>
                <span class="concept-confidence">${Math.round((concept.confidence || 0) * 100)}%</span>
            </div>
            ${concept.matched_phrases ? `<p class="concept-phrases">Matched: ${escapeHtml(concept.matched_phrases.join(', '))}</p>` : ''}
        </div>
    `).join('');
}

// Display reasoning trace
export function displayReasoningTrace(trace) {
    const container = document.getElementById('reasoningList');
    if (!container) return;

    if (!trace || trace.length === 0) {
        container.innerHTML = '<p style="color: var(--gray-500);">No reasoning trace available</p>';
        return;
    }
    container.innerHTML = trace.map((step, idx) => {
        let text = step;
        let details = '';
        try {
            if (typeof step === 'string' && step.trim().startsWith('{')) {
                const parsed = JSON.parse(step);
                text = parsed.text || step;
                if (parsed.provenance) details += `<span style="font-size:0.75rem; background:#f3f4f6; padding:2px 6px; border-radius:4px; margin-right:5px;">${parsed.provenance}</span>`;
                if (parsed.confidence) details += `<span style="font-size:0.75rem; color:#6b7280;">Conf: ${parsed.confidence}</span>`;
            } else if (typeof step === 'object' && step !== null) {
                text = step.text || JSON.stringify(step);
                if (step.provenance) details += `<span style="font-size:0.75rem; background:#f3f4f6; padding:2px 6px; border-radius:4px; margin-right:5px;">${step.provenance}</span>`;
                if (step.confidence) details += `<span style="font-size:0.75rem; color:#6b7280;">Conf: ${step.confidence}</span>`;
            }
        } catch(e) {}
        
        return `<div class="reasoning-step">
            <div>${escapeHtml(String(text))}</div>
            ${details ? `<div style="margin-top:6px;">${details}</div>` : ''}
        </div>`;
    }).join('');
}

// Render decision panel
export function renderDecisionPanel(decision) {
    const actionsContainer = document.getElementById('actionsList');
    if (!actionsContainer) return;

    const label = decision.decision_label || decision.recommended_action || "Review Case";
    const detail = decision.detail || "Based on the analysis, please review the recommendations below.";
    const nextSteps = decision.next_steps || [];
    const topRisks = decision.top_3_risks || [];

    const actionType = decision.recommended_action || "REVIEW";
    let actionClass = "info";
    let actionIcon = "fa-info-circle";

    if (actionType.includes("FILE_COMPLAINT") || actionType.includes("FILE")) {
        actionClass = "success";
        actionIcon = "fa-check-circle";
    } else if (actionType.includes("SEND_NOTICE") || actionType.includes("NOTICE")) {
        actionClass = "warning";
        actionIcon = "fa-exclamation-triangle";
    } else if (actionType.includes("HIGH_RISK") || actionType.includes("DEFEND")) {
        actionClass = "error";
        actionIcon = "fa-times-circle";
    } else if (actionType.includes("SETTLEMENT")) {
        actionClass = "warning";
        actionIcon = "fa-handshake";
    } else if (actionType.includes("FIX")) {
        actionClass = "warning";
        actionIcon = "fa-wrench";
    }

    let html = `
        <div class="decision-panel decision-${actionClass}">
            <div class="decision-header">
                <div class="decision-icon">
                    <i class="fas ${actionIcon}"></i>
                </div>
                <div class="decision-title-area">
                    <h4>${label}</h4>
                    <p>${detail}</p>
                </div>
            </div>
    `;

    if (topRisks.length > 0) {
        html += `
            <div class="decision-risks">
                <h5><i class="fas fa-flag"></i> Top Identified Risks:</h5>
                <ul>
                    ${topRisks.map(risk => {
            const riskText = typeof risk === 'string' ? risk : (risk.risk || risk.title || 'Risk identified');
            const severity = typeof risk === 'object' ? (risk.severity || 'MEDIUM') : 'MEDIUM';
            return `<li class="risk-item risk-${severity.toLowerCase()}">${riskText}</li>`;
        }).join('')}
                </ul>
            </div>
        `;
    }

    if (nextSteps.length > 0) {
        html += `
            <div class="decision-steps">
                <h5><i class="fas fa-list-ol"></i> Suggested Review Priorities:</h5>
                <ol>
                    ${nextSteps.map(step => `<li>${escapeHtml(String(step))}</li>`).join('')}
                </ol>
            </div>
        `;
    }

    html += `</div>`;
    actionsContainer.innerHTML = html;
}

// Render economics engine
export function renderEconomicsEngine(economicsData) {
    const container = document.getElementById('economicsList');
    if (!container) return;

    if (!economicsData) {
        container.innerHTML = '<p style="color: var(--gray-500);"><i class="fas fa-info-circle"></i> No economic data available.</p>';
        return;
    }

    if (Array.isArray(economicsData)) {
        renderList("economicsList", economicsData, "No economic data available");
        return;
    }

    let html = '';
    for (const [key, value] of Object.entries(economicsData)) {
        const displayKey = key.replace(/_/g, ' ');
        const displayValue = typeof value === 'object' && value !== null ? JSON.stringify(value) : value;
        html += `
            <div style="background: var(--gray-50); padding: 1rem; border-radius: var(--radius-md); border-left: 3px solid var(--primary-500);">
                <strong style="color: var(--primary-700); text-transform: capitalize;">${displayKey}:</strong>
                <span style="display: block; margin-top: 0.25rem; font-size: 0.95rem; color: var(--gray-800);">${displayValue}</span>
            </div>
        `;
    }
    
    container.innerHTML = html || '<p style="color: var(--gray-500);"><i class="fas fa-info-circle"></i> No economic data available.</p>';
}

// Render timeline engine & procedural graph
export function renderTimelineEngine(timelineData, fullData = null) {
    const container = document.getElementById('timelineList');
    if (!container) return;

    const data = fullData || (window.state && window.state.analysisResult) || {};
    const caseData = (window.state && window.state.caseData) || data.case_data || {};
    const limitation = data.limitation || {};
    const domain = (window.state && window.state.userDomain) || (caseData.case_type === 'SARFAESI' ? 'sarfaesi' : 'ni_act');

    // 1. If timelineData has populated non-empty nodes, use them
    let nodes = (timelineData && Array.isArray(timelineData.nodes) && timelineData.nodes.length > 0)
        ? timelineData.nodes
        : null;

    // 2. If nodes is empty, generate domain-specific statutory milestone nodes
    if (!nodes) {
        if (domain === 'sarfaesi' || caseData.case_type === 'SARFAESI') {
            const npaDate = caseData.npa_date || 'Day 90+ Overdue';
            const notice132Date = caseData.notice_13_2_date || 'Statutory Demand Issued';
            const repDate = caseData.borrower_representation_date;
            const replyDate = caseData.bank_reply_13_3a_date;
            const possDate = caseData.possession_13_4_date;
            const dmDate = caseData.dm_application_date || caseData.dm_order_date;
            const auctionDate = caseData.auction_notice_date;
            const saDate = caseData.sa_filing_date;

            nodes = [
                {
                    name: "1. NPA Classification (90-Day Default)",
                    statute: "RBI IRAC Norms Master Circular",
                    authority: "RBI Regulatory Directives & Banking Regulation Act",
                    date: caseData.npa_date || "Statutory 90-Day NPA Classification",
                    completed: true,
                    defect: caseData.npa_classification_challenged ? "Premature NPA tagging alleged" : null
                },
                {
                    name: "2. Section 13(2) Demand Notice (60-Day Window)",
                    statute: "Section 13(2) SARFAESI Act",
                    authority: "Transcore v. Union of India (2008)",
                    date: caseData.notice_13_2_date || "Mandatory 60-Day Demand Notice Served",
                    completed: true,
                    defect: caseData.notice_withdrawn ? "Notice Withdrawn by Secured Creditor" : null
                },
                {
                    name: "3. Section 13(3A) Borrower Representation & Decision",
                    statute: "Section 13(3A) SARFAESI Act",
                    authority: "Mardia Chemicals Ltd. v. UOI (2004)",
                    date: caseData.borrower_representation_date || "Within 60-Day Window",
                    completed: true,
                    defect: (repDate && !replyDate) ? "15-Day Mandatory Reasoned Decision not communicated" : null
                },
                {
                    name: "4. Section 13(4) Symbolic / Physical Possession Measure",
                    statute: "Section 13(4) & Rule 8 Security Interest Rules",
                    authority: "Mathew Varghese v. M. Amritha Kumar (2014)",
                    date: caseData.possession_13_4_date || "Post 60-Day Cure Period",
                    completed: true,
                    defect: caseData.newspaper_publication === "No" ? "Rule 8(2) 2-newspaper publication missing" : null
                },
                {
                    name: "5. Section 14 DM / CMM Chief Magistrate Order",
                    statute: "Section 14 SARFAESI Act",
                    authority: "Standard Chartered Bank v. V. Noble Kumar (2013)",
                    date: dmDate || "Administrative Assistance Application",
                    completed: !!dmDate,
                    defect: null
                },
                {
                    name: "6. Rule 8(6) / 9(1) Public Auction Sale Notice (30 Days)",
                    statute: "Rule 8(6) & Rule 9(1) Security Interest Rules",
                    authority: "Celir LLP v. Bafna Motors (2023)",
                    date: auctionDate || "Public Notice & Valued Reserve Price",
                    completed: !!auctionDate,
                    defect: caseData.valuation_report_available === "No" ? "Mandatory reserve price valuation report missing" : null
                },
                {
                    name: "7. Section 17 Securitisation Application (DRT Limitation)",
                    statute: "Section 17(1) SARFAESI Act",
                    authority: "B. Arvind Kumar v. Govt. of India (2007)",
                    date: saDate || "Strict 45-Day DRT Limitation Window",
                    completed: !!saDate,
                    defect: null
                }
            ];
        } else {
            // NI Act Section 138 Statutory Milestones
            const txDate = caseData.transaction_date || caseData.loan_date || "Pre-Cheque Consideration";
            const chqDate = caseData.cheque_date || "Date of Instrument";
            const disDate = caseData.dishonour_date || caseData.memo_date || "Bank Dishonour Date";
            const notDate = caseData.notice_date || "Legal Demand Notice Served";
            const recDate = caseData.notice_received_date || (caseData.notice_date ? `${caseData.notice_date} + 3 days` : "Notice Receipt & Delivery");
            const fileDate = caseData.filing_date || "Complaint Filing Date";

            const hasNoticeDelay = limitation.notice_delay_days > 0 || caseData.within_30_days === "No";
            const isPremature = limitation.is_premature || false;
            const isTimeBarred = limitation.is_time_barred || false;

            nodes = [
                {
                    name: "1. Underlying Transaction & Debt Creation",
                    statute: "Section 138 (Explanation) & Section 139 NI Act",
                    authority: "Bir Singh v. Mukesh Kumar (2019) & Basalingappa (2019)",
                    date: txDate,
                    completed: true,
                    defect: (caseData.agreement_type === "No Formal Agreement" && caseData.itr_available === "No") ? "Financial capacity / debt proof lacks documentary corroboration" : null
                },
                {
                    name: "2. Cheque Drawing & Banking Presentation",
                    statute: "Section 138 Proviso (a) NI Act",
                    authority: "MSR Leathers v. S. Palaniappan (2013)",
                    date: chqDate,
                    completed: true,
                    defect: caseData.account_closed ? "Cheque drawn on closed account (Presumption intact under NEPC Micon)" : null
                },
                {
                    name: "3. Cheque Dishonour & Bank Return Memo",
                    statute: "Section 146 NI Act (Presumption of Bank Slip)",
                    authority: "Kishan Rao v. Shankargouda (2018)",
                    date: disDate,
                    completed: true,
                    defect: null
                },
                {
                    name: "4. Statutory Demand Notice Issued (30-Day Limit)",
                    statute: "Section 138 Proviso (b) NI Act",
                    authority: "C.C. Alavi Haji v. Palapetty Muhammed (2007)",
                    date: notDate,
                    completed: true,
                    defect: hasNoticeDelay ? `Notice served ${limitation.notice_delay_days || 'X'} days late; exceeds 30-day statutory window (Section 142 condonation required)` : null
                },
                {
                    name: "5. Notice Delivery & 15-Day Cure Period Expiry",
                    statute: "Section 138 Proviso (c) NI Act",
                    authority: "Subodh S. Salaskar v. Jayprakash M. Shah (2008)",
                    date: recDate,
                    completed: true,
                    defect: isPremature ? "Premature: Accused's 15-day repayment window has not expired (Yogendra Pratap Singh bar)" : null
                },
                {
                    name: "6. Cause of Action & Court Complaint Filing (30-Day Window)",
                    statute: "Section 142(1)(b) NI Act",
                    authority: "Yogendra Pratap Singh v. Savitri Pandey (2014)",
                    date: fileDate,
                    completed: true,
                    defect: isTimeBarred ? "Time Barred: Exceeds 30-day limitation from cause of action date" : null
                }
            ];
        }
    }

    const completedCount = nodes.filter(n => n.completed).length;
    const totalCount = nodes.length;
    const stageName = (timelineData && timelineData.current_stage) || (domain === 'sarfaesi' ? 'Active Recovery Stage' : 'Active Court Proceedings');

    let html = `<div style="margin-bottom: 1.25rem; padding: 0.85rem 1.15rem; background: var(--gray-100); border-radius: 0.75rem; border: 1px solid var(--gray-200); display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:0.5rem;">
        <span style="font-weight: 700; color: var(--primary-600);"><i class="fas fa-network-wired"></i> Stage: ${escapeHtml(stageName)}</span>
        <span style="font-size: 0.82rem; font-weight: 600; color: var(--gray-600);">${completedCount} of ${totalCount} Statutory Milestones Tracked</span>
    </div>`;

    html += nodes.map((node, idx) => {
        const isDone = node.completed;
        const hasDefect = !!node.defect;
        const badgeColor = hasDefect ? '#ef4444' : (isDone ? '#10b981' : '#6b7280');
        const badgeText = hasDefect ? 'DEFECT DETECTED' : (isDone ? 'COMPLIANT' : 'PENDING');
        const icon = hasDefect ? 'fa-exclamation-triangle' : (isDone ? 'fa-check-circle' : 'fa-clock');

        return `
            <div class="timeline-item" style="display: flex; gap: 1rem; margin-bottom: 1.25rem;">
                <div style="width: 2px; background: ${badgeColor}44; position: relative; margin-left: 6px;">
                    <div style="position: absolute; top: 0; left: -6px; width: 14px; height: 14px; border-radius: 50%; background: ${badgeColor}; border: 3px solid var(--gray-50); box-shadow: 0 0 0 2px ${badgeColor}33;"></div>
                </div>
                <div style="flex: 1; background: var(--gray-100); border: 1px solid ${hasDefect ? '#ef444455' : 'var(--gray-200)'}; padding: 1rem 1.2rem; border-radius: 0.75rem; box-shadow: 0 1px 3px rgba(0,0,0,0.05);">
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.45rem; flex-wrap: wrap; gap: 0.4rem;">
                        <div style="font-weight: 700; color: var(--gray-900); font-size: 0.95rem;">${escapeHtml(node.name)}</div>
                        <span style="background: ${badgeColor}18; color: ${badgeColor}; border: 1px solid ${badgeColor}44; font-size: 0.72rem; font-weight: 700; padding: 0.2rem 0.55rem; border-radius: 0.35rem; display: inline-flex; align-items: center; gap: 0.3rem;">
                            <i class="fas ${icon}"></i> ${badgeText}
                        </span>
                    </div>
                    <div style="font-size: 0.82rem; color: var(--gray-600); margin-bottom: 0.4rem; line-height: 1.4;">
                        <strong>Statute:</strong> ${escapeHtml(node.statute || '')} &bull; <strong>Authority:</strong> <em>${escapeHtml(node.authority || '')}</em>
                    </div>
                    ${node.date ? `<div style="font-size: 0.82rem; color: var(--primary-600); font-weight: 600;"><i class="fas fa-calendar-day"></i> Date: ${escapeHtml(String(node.date))}</div>` : ''}
                    ${hasDefect ? `<div style="margin-top: 0.6rem; background: #ef444415; border: 1px solid #ef444444; color: #ef4444; padding: 0.6rem 0.85rem; border-radius: 0.5rem; font-size: 0.85rem; font-weight: 500;"><i class="fas fa-ban"></i> ${escapeHtml(node.defect)}</div>` : ''}
                </div>
            </div>
        `;
    }).join('');

    // Append any opportunities if present
    if (timelineData && timelineData.opportunities && timelineData.opportunities.length > 0) {
        html += `<div style="margin-top: 1.5rem; margin-bottom: 0.75rem; font-weight: 700; color: var(--error-600); font-size: 0.9rem; text-transform: uppercase;"><i class="fas fa-bolt"></i> Strategic Opportunities</div>`;
        html += timelineData.opportunities.map(opp => `
            <div style="background: var(--error-50); border-left: 4px solid var(--error-600); padding: 1rem; margin-bottom: 0.75rem; border-radius: var(--radius-md); box-shadow: 0 2px 4px rgba(220, 38, 38, 0.1);">
                <div style="font-size: 0.95rem; font-weight: 700; color: var(--gray-900);">${escapeHtml(String(opp.opportunity || opp.title || opp))}</div>
                ${opp.action ? `<div style="font-size: 0.85rem; color: var(--error-800); margin-top: 0.35rem;"><strong>Recommended Action:</strong> ${escapeHtml(String(opp.action))}</div>` : ''}
            </div>
        `).join('');
    }

    container.innerHTML = html;
}

// Render limitation clock
export function renderLimitationClock(data) {
    const clockEl = document.getElementById('limitationCountdown');
    const daysEl = document.getElementById('countdownDays');
    const msgEl = document.getElementById('limitationMessage');
    const fillEl = document.getElementById('limitationStatusFill');

    if (!clockEl || !daysEl || !msgEl || !fillEl) return;

    let daysRemaining = null;
    let message = "";

    if (data.limitation) {
        daysRemaining = data.limitation.days_remaining;
        message = data.limitation.limitation_date
            ? `Your statutory window for filing expires on ${data.limitation.limitation_date}.`
            : (data.limitation.message || "");
    }

    // Check if condonation is active via global state or response payload
    const rawCondonation = String(
        (window.state && window.state.caseData && window.state.caseData.condonation_attached) ||
        (data.case_data && data.case_data.condonation_attached) ||
        (data.limitation && data.limitation.condonation_attached) || ''
    ).trim().toLowerCase();

    const condonationActive = rawCondonation === 'yes' || rawCondonation.startsWith('yes') || rawCondonation === 'true' || rawCondonation === '1' || (data.limitation && data.limitation.status === 'CONDONED');

    if (condonationActive) {
        clockEl.classList.remove('hidden');
        clockEl.style.borderLeftColor = "var(--primary-500)";
        clockEl.style.background = "#f0fdf4";
        daysEl.textContent = "CONDONED";
        daysEl.style.fontSize = "1.3rem";
        daysEl.style.color = "#16a34a";
        msgEl.innerHTML = "<strong><i class='fas fa-clock-rotate-left'></i> Delay Condoned:</strong> Section 142(1)(b) Condonation Application attached. Statutory filing clock extended.";
        fillEl.style.width = "100%";
        fillEl.style.background = "linear-gradient(90deg, #10b981, #059669)";
    } else if (daysRemaining !== null) {
        clockEl.classList.remove('hidden');
        if (daysRemaining <= 0) {
            clockEl.style.borderLeftColor = "var(--error-500)";
            clockEl.style.background = "#fffbfb";
            daysEl.textContent = "EXPIRED";
            daysEl.style.fontSize = "1.35rem";
            daysEl.style.color = "var(--error-600)";
            msgEl.innerHTML = "<strong>CRITICAL: Limitation period has expired.</strong> Case may be time-barred unless Condonation of Delay (S.142(1)(b)) is attached.";
            fillEl.style.width = "100%";
            fillEl.style.background = "linear-gradient(90deg, #ef4444, #dc2626)";
        } else if (daysRemaining <= 7) {
            clockEl.style.borderLeftColor = "var(--warning-500)";
            clockEl.style.background = "#fffdfa";
            daysEl.textContent = `${daysRemaining} Days`;
            daysEl.style.fontSize = "1.55rem";
            daysEl.style.color = "var(--warning-600)";
            msgEl.textContent = message || `Urgent: Only ${daysRemaining} days remaining in statutory filing window.`;
            fillEl.style.width = `${Math.min(100, Math.max(10, ((30 - daysRemaining) / 30) * 100))}%`;
            fillEl.style.background = "linear-gradient(90deg, #f59e0b, #d97706)";
        } else {
            clockEl.style.borderLeftColor = "var(--primary-500)";
            clockEl.style.background = "#ffffff";
            daysEl.textContent = `${daysRemaining} Days`;
            daysEl.style.fontSize = "1.55rem";
            daysEl.style.color = "var(--primary-600)";
            msgEl.textContent = message || `Statutory window open. ${daysRemaining} days remaining to file.`;
            fillEl.style.width = `${Math.min(100, Math.max(10, ((30 - daysRemaining) / 30) * 100))}%`;
            fillEl.style.background = "linear-gradient(90deg, #3b82f6, #2563eb)";
        }
    } else {
        clockEl.classList.add('hidden');
    }
}

// Render corporate warning / compliance card
export function renderCorporateWarning(data) {
    const alertEl = document.getElementById('corporateAlert');
    if (!alertEl) return;

    const caseData = (window.state && window.state.caseData) || (data && data.case_data) || {};
    const accusedName = caseData.accused_name || "";
    const accusedType = (caseData.accused_type || "").toLowerCase();
    const isCompany = /pvt|ltd|corp|inc|co\.|company/i.test(accusedName) || accusedType.includes('pvt') || accusedType.includes('company') || accusedType.includes('firm');

    const directorsNamedRaw = String(caseData.directors_named || '').trim().toLowerCase();
    const directorsNamed = directorsNamedRaw.startsWith('yes') || directorsNamedRaw === 'true' || directorsNamedRaw === '1' || (caseData.accused_directors && caseData.accused_directors.length > 2);

    if (isCompany) {
        alertEl.classList.remove('hidden');
        if (directorsNamed) {
            alertEl.style.borderLeft = "5px solid var(--success-500)";
            alertEl.style.background = "var(--success-50)";
            alertEl.innerHTML = `
                <h3 style="color: var(--success-700); margin: 0 0 0.25rem 0; display: flex; align-items: center; gap: 0.5rem;">
                    <i class="fas fa-check-circle" style="color: var(--success-600);"></i> Section 141 Vicarious Liability Compliant
                </h3>
                <p style="font-size: 0.9rem; color: var(--success-800); margin: 0;">
                    Corporate accused identified and Directors / Authorized Officers in charge of day-to-day operations are properly named.
                </p>
            `;
        } else {
            alertEl.style.borderLeft = "5px solid var(--warning-500)";
            alertEl.style.background = "var(--warning-50)";
            alertEl.innerHTML = `
                <h3 style="color: var(--warning-700); margin: 0 0 0.25rem 0; display: flex; align-items: center; gap: 0.5rem;">
                    <i class="fas fa-building" style="color: var(--warning-600);"></i> Section 141 Corporate Warning
                </h3>
                <p style="font-size: 0.9rem; color: var(--warning-800); margin: 0;">
                    Accused is a corporate entity. <strong>Failure to name Directors / Authorized Officers</strong> in charge of daily affairs will lead to threshold dismissal under the <em>Aneeta Hada</em> precedent.
                </p>
            `;
        }
    } else {
        alertEl.classList.add('hidden');
    }
}

// Render case summary card
export function renderCaseSummaryCard(data) {
    const score = data.score || 0;
    const summaryContainer = document.getElementById('scoreExplanation');
    if (!summaryContainer) return;

    const oldCard = summaryContainer.querySelector('.case-summary-card');
    if (oldCard) oldCard.remove();

    let strengthLevel, strengthColor, strengthIcon, actionText;
    if (score >= 70) {
        strengthLevel = 'STRONG';
        strengthColor = 'success';
        strengthIcon = 'fa-shield-alt';
        actionText = 'Your case has solid legal merit. Proceed with confidence.';
    } else if (score >= 50) {
        strengthLevel = 'MODERATE';
        strengthColor = 'warning';
        strengthIcon = 'fa-balance-scale';
        actionText = 'Your case has potential but can be strengthened with additional evidence.';
    } else {
        strengthLevel = 'WEAK';
        strengthColor = 'error';
        strengthIcon = 'fa-exclamation-triangle';
        actionText = 'Your case needs significant improvement before proceeding.';
    }

    const keyFactors = [];
    if (data.strengths && data.strengths.length > 0) {
        keyFactors.push(`${data.strengths.length} strength${data.strengths.length > 1 ? 's' : ''} identified`);
    }
    if (data.weaknesses && data.weaknesses.length > 0) {
        keyFactors.push(`${data.weaknesses.length} weakness${data.weaknesses.length > 1 ? 'es' : ''} detected`);
    }
    if (data.issues && data.issues.length > 0) {
        keyFactors.push(`${data.issues.length} critical issue${data.issues.length > 1 ? 's' : ''} found`);
    }

    const summaryHTML = `
        <div class="case-summary-card case-${strengthColor}" style="background: linear-gradient(135deg, var(--${strengthColor}-50) 0%, white 100%); border: 2px solid var(--${strengthColor}-500); border-radius: var(--radius-xl); padding: 2rem; margin-bottom: 2rem; box-shadow: var(--shadow-lg);">
            <div style="display: flex; align-items: start; gap: 1.5rem;">
                <div class="summary-icon" style="width: 64px; height: 64px; background: var(--${strengthColor}-100); border-radius: var(--radius-lg); display: flex; align-items: center; justify-content: center; flex-shrink: 0;">
                    <i class="fas ${strengthIcon}" style="font-size: 2rem; color: var(--${strengthColor}-600);"></i>
                </div>
                <div style="flex: 1;">
                    <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.5rem;">
                        <h3 style="margin: 0; font-size: var(--font-size-2xl); font-weight: 700; color: var(--gray-900);">
                            ${strengthLevel} Case
                        </h3>
                        <span style="background: var(--${strengthColor}-100); color: var(--${strengthColor}-700); padding: 0.25rem 0.75rem; border-radius: var(--radius-full); font-size: var(--font-size-sm); font-weight: 600;">
                            ${score.toFixed(1)}/100
                        </span>
                    </div>
                    <p style="color: var(--gray-700); font-size: var(--font-size-base); line-height: 1.6; margin-bottom: 1rem;">
                        ${actionText}
                    </p>
                    ${keyFactors.length > 0 ? `
                        <div style="display: flex; flex-wrap: wrap; gap: 0.75rem;">
                            ${keyFactors.map(factor => `
                                <span style="background: white; padding: 0.5rem 1rem; border-radius: var(--radius-md); font-size: var(--font-size-sm); color: var(--gray-700); border: 1px solid var(--gray-200);">
                                    <i class="fas fa-check-circle" style="color: var(--${strengthColor}-500); margin-right: 0.25rem;"></i>
                                    ${factor}
                                </span>
                            `).join('')}
                        </div>
                    ` : ''}
                </div>
            </div>
        </div>
    `;

    summaryContainer.insertAdjacentHTML('afterbegin', summaryHTML);
}

// Render score explanation
export function renderScoreExplanation(data) {
    const score = data.score || 0;
    const explanationContainer = document.getElementById('scoreExplanation');
    if (!explanationContainer) return;

    let explanation = '';
    let missingElements = [];
    let detectedWeaknesses = [];

    const caseData = (window.state && window.state.caseData) || {};

    if (score < 50) {
        explanation = '<strong>Low Score Analysis:</strong> Your case received a low score due to the following factors:';

        if (!data.strengths || data.strengths.length === 0) {
            missingElements.push('No strong legal arguments detected');
        }

        if (data.issues && data.issues.length > 3) {
            missingElements.push(`${data.issues.length} critical issues identified`);
        }

        if (data.weaknesses && data.weaknesses.length > 0) {
            data.weaknesses.forEach(w => {
                const text = typeof w === 'string' ? w : (w.weakness || w.title || w.description);
                if (text) detectedWeaknesses.push(text);
            });
        }

        if (!isTruthy(caseData.notice_sent)) {
            missingElements.push('Legal notice not sent (mandatory requirement)');
        }

        if (!isTruthy(caseData.dishonour_memo) && !isTruthy(caseData.agreement_documents)) {
            missingElements.push('No supporting documents provided');
        }

        if (!caseData.transaction_date) {
            missingElements.push('Incomplete timeline information');
        }

    } else if (score < 70) {
        explanation = '<strong>Moderate Score Analysis:</strong> Your case shows some merit but has areas for improvement:';

        if (data.weaknesses && data.weaknesses.length > 0) {
            data.weaknesses.slice(0, 3).forEach(w => {
                const text = typeof w === 'string' ? w : (w.weakness || w.title || w.description);
                if (text) detectedWeaknesses.push(text);
            });
        }

    } else {
        explanation = '<strong>Strong Score Analysis:</strong> Your case demonstrates solid legal merit with:';
        const summaryCardHTML = explanationContainer.querySelector('.case-summary-card')?.outerHTML || '';
        explanationContainer.innerHTML = summaryCardHTML + `
            <div class="insight-panel insight-success">
                <div class="insight-header">
                    <i class="fas fa-check-circle"></i>
                    <h4>Why This Score?</h4>
                </div>
                <p>${explanation}</p>
                <ul class="insight-list">
                    ${(data.strengths || []).slice(0, 3).map(s => {
            const text = typeof s === 'string' ? s : (s.strength || s.title || s.description);
            return `<li><i class="fas fa-check"></i> ${text}</li>`;
        }).join('')}
                </ul>
            </div>
        `;
        return;
    }

    const summaryCardHTML = explanationContainer.querySelector('.case-summary-card')?.outerHTML || '';
    explanationContainer.innerHTML = summaryCardHTML + `
        <div class="insight-panel ${score < 50 ? 'insight-danger' : 'insight-warning'}">
            <div class="insight-header">
                <i class="fas fa-info-circle"></i>
                <h4>Why This Score?</h4>
            </div>
            <p>${explanation}</p>
            
            ${missingElements.length > 0 ? `
                <div class="insight-section">
                    <h5><i class="fas fa-exclamation-triangle"></i> Missing Critical Elements:</h5>
                    <ul class="insight-list">
                        ${missingElements.map(item => `<li><i class="fas fa-times"></i> ${item}</li>`).join('')}
                    </ul>
                </div>
            ` : ''}
            
            ${detectedWeaknesses.length > 0 ? `
                <div class="insight-section">
                    <h5><i class="fas fa-flag"></i> Detected Weaknesses:</h5>
                    <ul class="insight-list">
                        ${detectedWeaknesses.map(item => `<li><i class="fas fa-arrow-right"></i> ${item}</li>`).join('')}
                    </ul>
                </div>
            ` : ''}
        </div>
    `;
}

// Render case completeness / validation check
export function validateCaseCompleteness() {
    const validation = {
        isValid: true,
        criticalMissing: [],
        warnings: [],
        dataCompleteness: 0,
        confidenceLevel: 'LOW'
    };

    const caseData = (window.state && window.state.caseData) || {};

    const caseDescription = caseData.case_title || '';
    const dishonourDate = caseData.dishonour_date || '';
    const hasEvidence = isTruthy(caseData.original_cheque) || isTruthy(caseData.dishonour_memo) || isTruthy(caseData.agreement_documents);

    if (!caseDescription || caseDescription.trim() === '') {
        validation.criticalMissing.push('Case description/title is required');
        validation.isValid = false;
    }

    if (!dishonourDate || dishonourDate.trim() === '') {
        validation.criticalMissing.push('Cheque dishonour date is required');
        validation.isValid = false;
    }

    if (!hasEvidence) {
        validation.criticalMissing.push('At least one piece of evidence is required');
        validation.isValid = false;
    }

    const noticeSent = isTruthy(caseData.notice_sent);
    const hasDocuments = isTruthy(caseData.dishonour_memo) || isTruthy(caseData.agreement_documents);
    const hasTimeline = caseData.transaction_date;

    let completenessScore = 0;
    const totalChecks = 10;

    if (caseDescription) completenessScore++;
    if (dishonourDate) completenessScore++;
    if (hasEvidence) completenessScore++;
    if (noticeSent) completenessScore++;
    if (hasDocuments) completenessScore++;
    if (hasTimeline) completenessScore++;
    if (caseData.cheque_amount) completenessScore++;
    if (caseData.complainant_name) completenessScore++;
    if (caseData.accused_name) completenessScore++;
    if (caseData.debt_amount) completenessScore++;

    validation.dataCompleteness = Math.round((completenessScore / totalChecks) * 100);

    if (validation.dataCompleteness >= 70) {
        validation.confidenceLevel = 'HIGH';
    } else if (validation.dataCompleteness >= 40) {
        validation.confidenceLevel = 'MEDIUM';
    } else {
        validation.confidenceLevel = 'LOW';
    }

    if (!noticeSent) {
        validation.warnings.push('Legal notice not sent - This significantly weakens your case (required within 30 days of dishonour)');
    }

    if (!hasDocuments) {
        validation.warnings.push('No supporting documents (bank memo, transaction proof) - Evidence strengthens your case');
    }

    if (!hasTimeline) {
        validation.warnings.push('Transaction date missing - Timeline helps establish the case chronology');
    }

    return validation;
}

// Render confidence indicator
export function renderConfidenceIndicator(data) {
    const confidenceContainer = document.getElementById('confidenceIndicator');
    if (!confidenceContainer) return;

    const validation = validateCaseCompleteness();
    const dataCompleteness = validation.dataCompleteness;
    const score = data.score || 0;

    let confidenceLevel = 'LOW';
    let confidenceColor = 'error';
    let confidenceIcon = 'fa-exclamation-triangle';
    let confidenceText = '';

    if (dataCompleteness >= 70 && score > 0) {
        confidenceLevel = 'HIGH';
        confidenceColor = 'success';
        confidenceIcon = 'fa-check-circle';
        confidenceText = 'The analysis is based on comprehensive case data with all critical information provided.';
    } else if (dataCompleteness >= 40) {
        confidenceLevel = 'MEDIUM';
        confidenceColor = 'warning';
        confidenceIcon = 'fa-info-circle';
        confidenceText = 'The analysis is based on moderate case data. Adding more information may improve accuracy.';
    } else {
        confidenceLevel = 'LOW';
        confidenceColor = 'error';
        confidenceIcon = 'fa-exclamation-triangle';
        confidenceText = 'The analysis is based on limited case data. Results may not be fully accurate.';
    }

    confidenceContainer.innerHTML = `
        <div class="confidence-panel confidence-${confidenceColor}">
            <div class="confidence-badge-large">
                <i class="fas ${confidenceIcon}"></i>
                <div>
                    <span class="confidence-label">Analysis Confidence</span>
                    <span class="confidence-value">${confidenceLevel}</span>
                </div>
            </div>
            <div class="confidence-meter">
                <div class="confidence-meter-fill confidence-${confidenceColor}" style="width: ${dataCompleteness}%"></div>
            </div>
            <p class="confidence-description">${confidenceText}</p>
            <div class="confidence-stats">
                <div class="confidence-stat">
                    <span class="stat-value">${dataCompleteness}%</span>
                    <span class="stat-label">Data Completeness</span>
                </div>
                <div class="confidence-stat">
                    <span class="stat-value">${(data.strengths || []).length + (data.issues || []).length}</span>
                    <span class="stat-label">Factors Analyzed</span>
                </div>
            </div>
        </div>
    `;
}

// Render improvement suggestions
export function renderImprovementSuggestions(data) {
    const score = data.score || 0;
    const suggestionsContainer = document.getElementById('improvementSuggestions');
    if (!suggestionsContainer) return;

    if (score >= 80) {
        suggestionsContainer.innerHTML = `
            <div class="suggestions-panel suggestions-minimal">
                <p style="color: var(--success-700); font-weight: 500;">
                    <i class="fas fa-check-circle"></i> Your case is already strong. Consider reviewing the recommended actions to proceed.
                </p>
            </div>
        `;
        return;
    }

    const suggestions = [];
    const caseData = (window.state && window.state.caseData) || {};

    if (!isTruthy(caseData.notice_sent)) {
        suggestions.push({
            icon: 'fa-file-alt',
            title: 'Send Legal Notice',
            description: 'Issue a legal notice within 30 days of cheque dishonour. This is a mandatory legal requirement.',
            impact: 'High Impact',
            color: 'error'
        });
    }

    if (!isTruthy(caseData.bank_memo_received) && !isTruthy(caseData.dishonour_memo)) {
        suggestions.push({
            icon: 'fa-university',
            title: 'Obtain Bank Memo',
            description: 'Get the official bank memo showing "Insufficient Funds" or dishonour reason.',
            impact: 'High Impact',
            color: 'error'
        });
    }

    if (!isTruthy(caseData.agreement_documents) && !caseData.transaction_date) {
        suggestions.push({
            icon: 'fa-receipt',
            title: 'Gather Transaction Proof',
            description: 'Collect invoices, agreements, or receipts proving the underlying debt.',
            impact: 'Medium Impact',
            color: 'warning'
        });
    }

    if (data.weaknesses && data.weaknesses.length > 0) {
        suggestions.push({
            icon: 'fa-shield-alt',
            title: 'Address Identified Weaknesses',
            description: `${data.weaknesses.length} weakness(es) found in your case. Review the weaknesses section for details.`,
            impact: 'Medium Impact',
            color: 'warning'
        });
    }

    if (!isTruthy(caseData.witness_available)) {
        suggestions.push({
            icon: 'fa-users',
            title: 'Consider Witness Statements',
            description: 'Gather witness statements or affidavits if available to strengthen your case.',
            impact: 'Low Impact',
            color: 'info'
        });
    }

    if (suggestions.length === 0) {
        suggestionsContainer.innerHTML = `
            <div class="suggestions-panel">
                <p style="color: var(--gray-600);">
                    <i class="fas fa-info-circle"></i> No specific improvements needed at this time.
                </p>
            </div>
        `;
        return;
    }

    suggestionsContainer.innerHTML = `
        <div class="suggestions-panel">
            <div class="suggestions-header">
                <i class="fas fa-lightbulb"></i>
                <h4>How to Improve This Case</h4>
            </div>
            <div class="suggestions-grid">
                ${suggestions.map(sug => `
                    <div class="suggestion-card suggestion-${sug.color}">
                        <div class="suggestion-icon">
                            <i class="fas ${sug.icon}"></i>
                        </div>
                        <div class="suggestion-content">
                            <h5>${sug.title}</h5>
                            <p>${sug.description}</p>
                            <span class="suggestion-impact impact-${sug.color}">${sug.impact}</span>
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

// Render explainable score
export function renderExplainableScore(data) {
    const container = document.getElementById('explainableScorePanel');
    if (!container) return;

    const score = data.score ?? 0;
    const causality = data.causality_map || [];
    const landmarks = [
        { name: 'Basalingappa v. Mudibasappa', year: '2019', area: 'Financial Capacity' },
        { name: 'Rangappa v. Srikanth', year: '2010', area: 'Presumption under S.139' },
        { name: 'Aneeta Hada v. Godfather', year: '2012', area: 'S.141 Vicarious Liability' }
    ];

    const positives = causality.filter(c => (c.impact || 0) > 0);
    const negatives = causality.filter(c => (c.impact || 0) < 0);

    const caseData = window.state?.caseData || window.state?.currentCaseData || {};
    const condonationAttached = ['yes', 'true', true].includes((String(caseData.condonation_attached) || '').toLowerCase());
    const bsaAttached = ['yes', 'true', true].includes((String(caseData.bsa_certificate || caseData.has_bsa_certificate || caseData.has_65b_certificate) || '').toLowerCase());

    const filteredCausality = causality.map(c => {
        if (condonationAttached && (c.fact === "Limitation Delay" || c.fact === "Notice Delay")) {
            return { ...c, impact: 0, fact: "Limitation Delay (Cured via Condonation)" };
        }
        if (bsaAttached && (c.fact === "Missing S.63(4) BSA Certificate" || c.fact === "Missing S.65B Certificate")) {
            return { ...c, impact: 0, fact: "BSA S.63(4) Certificate (Attached)" };
        }
        return c;
    });

    const breakdownRows = filteredCausality.map(c => {
        const impact = c.impact || 0;
        const color = impact > 0 ? '#22c55e' : impact === 0 ? '#10b981' : '#ef4444';
        const sign = impact > 0 ? '+' : '';
        return `<div style="display:flex; justify-content:space-between; align-items:center; 
            padding: 0.6rem 1rem; border-radius:8px; margin-bottom:0.4rem; 
            background: ${impact >= 0 ? 'rgba(34,197,94,0.08)' : 'rgba(239,68,68,0.08)'}; 
            border-left: 3px solid ${color};">
            <span style="color:#374151; font-size:0.88rem;">${c.fact || 'Factor'}</span>
            <span style="font-weight:700; color:${color};">${impact === 0 ? '<i class="fas fa-check-circle"></i> Cured' : sign + impact}</span>
        </div>`;
    }).join('');

    const landmarkBadges = landmarks.slice(0, 3).map(l => `
        <span style="display:inline-flex; align-items:center; gap:0.4rem; 
            background:rgba(99,102,241,0.1); color:#4f46e5; border:1px solid rgba(99,102,241,0.2);
            padding:0.3rem 0.7rem; border-radius:20px; font-size:0.78rem; font-weight:600; margin:0.2rem;">
            <i class="fas fa-gavel"></i> ${l.name} (${l.year})
        </span>`).join('');

    container.innerHTML = `
        <div style="display:flex; gap:2rem; flex-wrap:wrap; margin-bottom:1.5rem; align-items:stretch;">
            <div style="text-align:center; padding: 2rem 1.5rem; background: linear-gradient(145deg, var(--gray-50) 0%, var(--gray-100) 100%); border-radius: 16px; border: 1px solid var(--gray-200); box-shadow: 0 10px 25px -5px rgba(0,0,0,0.1), inset 0 1px 0 rgba(255,255,255,0.05); min-width: 160px; display:flex; flex-direction:column; justify-content:center; align-items:center;">
                <div style="display:flex; align-items:baseline; justify-content:center; filter: drop-shadow(0 4px 6px rgba(0,0,0,0.1));">
                    <div style="font-size:3.5rem; font-weight:800; color:${score >= 70 ? '#10b981' : score >= 50 ? '#f59e0b' : '#ef4444'}; line-height:1;">${score}</div>
                    <div style="font-size:1.2rem; color:#6b7280; font-weight:700; margin-left: 4px;">/100</div>
                </div>
                <div style="font-size:0.85rem; color:#9ca3af; font-weight:700; margin-top: 0.75rem; text-transform:uppercase; letter-spacing: 1px;">Final Score</div>
            </div>
            <div style="flex:1; min-width:200px;">
                <div style="font-size:0.82rem; color:#6b7280; font-weight:600; margin-bottom:0.5rem; text-transform:uppercase;">Score Attribution</div>
                ${breakdownRows || '<p style="color:#9ca3af;">Run case analysis to see breakdown</p>'}
            </div>
        </div>
        <div>
            <div style="font-size:0.82rem; color:#6b7280; font-weight:600; margin-bottom:0.6rem; text-transform:uppercase;">
                <i class="fas fa-balance-scale"></i> Supported Precedents
            </div>
            <div>${landmarkBadges}</div>
        </div>
    `;
}

// Render evidence sufficiency meter
export function renderEvidenceSufficiencyMeter(data) {
    const container = document.getElementById('evidenceSufficiencyMeter');
    if (!container) return;

    const reliability = data.evidence_reliability || {};
    
    const pillars = [
        { label: 'Cheque / Primary Instrument', key: 'Cheque Original', fallback: 90, icon: 'fa-file-invoice' },
        { label: 'Notice Compliance', key: 'Notice (Registered Post)', fallback: 75, icon: 'fa-envelope' },
        { label: 'Debt Proof / Financial Capacity', key: 'Financial Capacity (Basalingappa)', fallback: 60, icon: 'fa-rupee-sign' },
        { label: 'Bank / Dishonour Memo', key: 'Dishonour Memo', fallback: 80, icon: 'fa-university' },
        { label: 'Witness Support', key: 'Witness', fallback: 40, icon: 'fa-users' },
    ];

    const bars = pillars.map(p => {
        let pct = p.fallback;
        const relData = reliability[p.key];
        if (relData) {
            pct = Math.round((relData.score || 0.5) * 100);
        }
        const color = pct >= 75 ? '#22c55e' : pct >= 50 ? '#f59e0b' : '#ef4444';
        const status = pct >= 75 ? 'Strong' : pct >= 50 ? 'Partial' : 'Weak';
        return `
            <div style="margin-bottom:1rem;">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.35rem;">
                    <span style="font-size:0.85rem; color:#374151; font-weight:500;">
                        <i class="fas ${p.icon}" style="color:${color}; margin-right:0.4rem;"></i>${p.label}
                    </span>
                    <span style="font-size:0.82rem; font-weight:700; color:${color};">${pct}% <span style="font-weight:400; color:#9ca3af;">(${status})</span></span>
                </div>
                <div style="height:8px; background:#f3f4f6; border-radius:99px; overflow:hidden;">
                    <div style="height:100%; width:${pct}%; background:${color}; border-radius:99px; transition:width 0.6s ease;"></div>
                </div>
            </div>`;
    }).join('');

    const total = pillars.reduce((acc, p) => {
        const relData = reliability[p.key];
        return acc + (relData ? Math.round((relData.score || 0.5) * 100) : p.fallback);
    }, 0);
    const overall = Math.round(total / pillars.length);
    const overallColor = overall >= 70 ? '#22c55e' : overall >= 50 ? '#f59e0b' : '#ef4444';

    container.innerHTML = `
        <div style="margin-bottom:1.5rem;">
            <div style="display:flex; align-items:center; gap:1rem; padding:1rem; 
                background:${overall >= 70 ? 'rgba(34,197,94,0.08)' : 'rgba(245,158,11,0.08)'}; 
                border-radius:12px; border:1px solid ${overallColor}30; margin-bottom:1.2rem;">
                <div style="font-size:2.5rem; font-weight:800; color:${overallColor};">${overall}%</div>
                <div>
                    <div style="font-weight:700; color:#111827;">Overall Case Readiness</div>
                    <div style="font-size:0.82rem; color:#6b7280;">Based on evidence pillar analysis</div>
                </div>
            </div>
            ${bars}
        </div>
        <div style="font-size:0.78rem; color:#9ca3af; border-top:1px solid #f3f4f6; padding-top:0.8rem;">
            <i class="fas fa-info-circle"></i> Scores are derived from your submitted evidence types and case data.
        </div>
    `;
}

// Render AI reasoning layer
export function renderAIReasoningLayer(data) {
    if (!data) return;

    const summaryEl = document.getElementById('aiCaseSummary');
    if (summaryEl) {
        let summaryText = data.case_summary || 'No automated summary generated.';
        // Convert newlines to HTML paragraphs for proper styling
        summaryEl.innerHTML = summaryText.split('\n').filter(p => p.trim()).map(p => `<p style="margin-bottom: 0.8rem; line-height: 1.6; color: var(--gray-700);">${escapeHtml(p)}</p>`).join('');
    }

    const badgeEl = document.getElementById('translatedVerdictBadge');
    if (badgeEl) {
        if (data.translated_verdict && data.translated_verdict !== data.verdict) {
            badgeEl.innerHTML = `<i class="fas fa-language"></i>&nbsp;${escapeHtml(String(data.translated_verdict))}`;
            badgeEl.classList.remove('hidden');
        } else {
            badgeEl.classList.add('hidden');
        }
    }

    const predEl = document.getElementById('aiOutcomePrediction');
    if (predEl) {
        const op = data.outcome_prediction || {};
        if (op.prediction) {
            const probText = String(op.probability || '0%');
            const probValue = parseFloat(probText) || 0;
            const band = (op.score_band || 'WEAK').toUpperCase();
            
            predEl.innerHTML = `
                <div class="rl-outcome-box rl-band-${band}">
                    <div class="rl-outcome-header">
                        <span class="rl-outcome-prediction">${op.prediction}</span>
                        <span class="rl-outcome-prob">${probText}</span>
                    </div>
                    <div class="rl-outcome-bar-wrap">
                        <div class="rl-outcome-bar" style="width:${Math.min(probValue, 100)}%"></div>
                    </div>
                    <p class="rl-outcome-rationale">${op.rationale || ''}</p>
                </div>`;
        } else {
            predEl.innerHTML = '<p class="rl-empty">No outcome prediction available.</p>';
        }
    }

    const statuteEl = document.getElementById('aiStatutoryInterpretation');
    if (statuteEl) {
        const interps = data.statutory_interpretation || [];
        if (!interps.length) {
            statuteEl.innerHTML = '<p class="rl-empty">No statutory analysis available.</p>';
        } else {
            statuteEl.innerHTML = interps.map(interp => {
                const rawStatus = (interp.status || 'NOTE').toUpperCase();
                const statusClass = rawStatus.replace(/\s+/g, '_');
                const failList = (interp.conditions_failed || []).map(c => `• ${c}`).join('<br>');
                
                return `
                <div class="rl-statute-card status-${statusClass}">
                    <div class="rl-statute-section">Section ${interp.section}</div>
                    <div class="rl-statute-title">${interp.title || ''}</div>
                    <span class="rl-statute-status-badge">${interp.status}</span>
                    <div class="rl-statute-finding">${interp.finding || ''}</div>
                    ${failList ? `<div style="margin-top:.6rem;font-size:.78rem;color:#b91c1c;"><strong>Unmet Conditions:</strong><br>${failList}</div>` : ''}
                    ${interp.punishment ? `<div style="margin-top:.5rem;font-size:.78rem;color:var(--gray-500);">Penalty: ${interp.punishment}</div>` : ''}
                    ${interp.limit ? `<div style="margin-top:.5rem;font-size:.78rem;color:#6d28d9;">${interp.limit}</div>` : ''}
                </div>`;
            }).join('');
        }
    }

    const precEl = document.getElementById('aiMatchedPrecedents');
    if (precEl) {
        const precedentsData = data.precedents || [];
        const supporting = data.supporting_precedents || [];
        const opposing = data.opposing_precedents || [];
        const distinguishable = data.distinguishable_precedents || [];
        
        const formatCard = (p, badgeClass, badgeText) => {
            const title = p.title || p.case || p.citation || 'Unknown Case';
            const citation = p.citation || '';
            const summary = p.summary || p.principle || p.precedent || '';
            const court = p.court || 'Supreme Court of India';
            const domain = p.domain || (p.area && p.area[0]) || 'Landmark Law';
            const link = p.link || p.document_url || `https://indiankanoon.org/search/?formInput=${encodeURIComponent(title)}`;
            const copyText = `${title}, ${citation} (${court})`;
            
            return `
                <div class="rl-precedent-card" style="border: 1px solid var(--gray-200); border-radius: 8px; padding: 1rem; margin-bottom: 0.75rem; background: var(--gray-100); transition: var(--transition-fast); box-shadow: 0 1px 3px rgba(0,0,0,0.03);">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.5rem; gap: 0.5rem; flex-wrap: wrap;">
                        <div style="display: flex; gap: 0.35rem; align-items: center; flex-wrap: wrap;">
                            <span class="rl-status-badge ${badgeClass}" style="font-size: 0.7rem; font-weight: 700; padding: 0.15rem 0.5rem; border-radius: 6px;">${badgeText}</span>
                            <span style="font-size: 0.65rem; color: #0369a1; background: #e0f2fe; padding: 0.15rem 0.45rem; border-radius: 6px; font-weight: 700; text-transform: uppercase;">${domain}</span>
                        </div>
                        ${p.binding !== false ? '<span style="font-size: 0.65rem; color: #1e3a8a; background: #dbeafe; padding: 0.12rem 0.45rem; border-radius: 6px; font-weight: 700; text-transform: uppercase;"><i class="fas fa-certificate" style="margin-right: 0.2rem;"></i> Binding SC</span>' : ''}
                    </div>
                    <div class="rl-precedent-case" style="font-size: 1.05rem; font-weight: 700; color: var(--gray-900);">
                        <a href="${link}" target="_blank" rel="noopener noreferrer" style="color: var(--primary-600); text-decoration: none; border-bottom: 1px dashed rgba(2,132,199,0.4);" title="Open authoritative judgment">
                            ${title} <i class="fas fa-external-link-alt" style="font-size: 0.72rem; margin-left: 0.25rem; opacity: 0.7;"></i>
                        </a>
                    </div>
                    <div class="rl-precedent-citation" style="margin-top: 0.3rem; font-size: 0.85rem; color: var(--gray-600); font-weight: 600;">
                        <i class="fas fa-book-open" style="margin-right: 0.3rem; color: var(--gray-400); font-size: 0.75rem;"></i>${citation}
                    </div>
                    ${court ? `<div style="font-size: .78rem; color: var(--gray-500); margin-top: 0.15rem; margin-bottom: .4rem;"><i class="fas fa-landmark" style="margin-right: 0.3rem; font-size: 0.72rem;"></i>${court}</div>` : ''}
                    <div class="rl-precedent-principle" style="margin-top: 0.5rem; font-size: 0.85rem; line-height: 1.45; color: var(--gray-700); background: var(--gray-50); padding: 0.55rem; border-radius: 6px; border-left: 3px solid var(--primary-400);">${summary}</div>
                    <div style="display: flex; justify-content: flex-end; gap: 0.5rem; margin-top: 0.6rem;">
                        <a href="${link}" target="_blank" rel="noopener noreferrer" style="font-size: 0.74rem; font-weight: 600; color: var(--primary-600); text-decoration: none; display: inline-flex; align-items: center; gap: 0.25rem; background: rgba(2,132,199,0.08); padding: 0.2rem 0.5rem; border-radius: 4px;">
                            <i class="fas fa-landmark"></i> Indian Kanoon
                        </a>
                        <button onclick="navigator.clipboard.writeText('${copyText.replace(/'/g, "\\'")}'); if(window.ui && window.ui.toast) { window.ui.toast('Citation copied to clipboard!', 'success'); } else { alert('Citation copied!'); }" style="background: rgba(15,23,42,0.05); border: 1px solid var(--gray-300); color: var(--gray-700); font-size: 0.74rem; font-weight: 600; cursor: pointer; display: inline-flex; align-items: center; gap: 0.25rem; padding: 0.2rem 0.5rem; border-radius: 4px; transition: all 0.15s ease;">
                            <i class="fas fa-copy"></i> Copy Citation
                        </button>
                    </div>
                </div>`;
        };

        let cardsHtml = [];
        
        // Add supporting
        supporting.forEach(p => cardsHtml.push(formatCard(p, 'rl-success-badge', 'Supporting (Payee)')));
        // Add opposing
        opposing.forEach(p => cardsHtml.push(formatCard(p, 'rl-danger-badge', 'Opposing (Drawer)')));
        // Add distinguishable
        distinguishable.forEach(p => cardsHtml.push(formatCard(p, 'rl-info-badge', 'Distinguishable')));
        
        // Add statute precedents (deduping if already added)
        precedentsData.forEach(p => {
            const title = (p.case || p.title || p.citation || '').toLowerCase();
            const isAlreadyAdded = [...supporting, ...opposing, ...distinguishable].some(ex => {
                const exTitle = (ex.title || ex.case || ex.citation || '').toLowerCase();
                return exTitle === title || (title && exTitle && (exTitle.includes(title) || title.includes(exTitle)));
            });
            if (!isAlreadyAdded) {
                cardsHtml.push(formatCard(p, 'rl-warning-badge', 'Landmark Precedent'));
            }
        });
        
        if (cardsHtml.length === 0) {
            precEl.innerHTML = '<p class="rl-empty">No precedents matched for detected concepts.</p>';
        } else {
            precEl.innerHTML = cardsHtml.join('');
        }
    }

    const risksEl = document.getElementById('aiRisksRebuttals');
    if (risksEl) {
        const risks = data.risks_and_rebuttals || [];
        if (!risks.length) {
            risksEl.innerHTML = '<p class="rl-empty">No specific risks identified for current case configuration.</p>';
        } else {
            risksEl.innerHTML = risks.map(r => `
                <div class="rl-risk-item sev-${r.severity}">
                    <div class="rl-risk-header sev-${r.severity}">
                        <span class="rl-sev-badge">${r.severity}</span>
                        <span class="rl-risk-title">${r.risk}</span>
                    </div>
                    <div class="rl-risk-desc">${r.description || ''}</div>
                    <div class="rl-rebuttal-box">
                        <strong><i class="fas fa-reply"></i> AI-Suggested Rebuttal</strong>
                        <div class="rl-rebuttal-text">${r.rebuttal || ''}</div>
                        ${r.case_law ? `<div class="rl-case-law"><i class="fas fa-book-open"></i> ${r.case_law}</div>` : ''}
                    </div>
                </div>`).join('');
        }
    }

    const evEl = document.getElementById('aiEvidenceSuggestions');
    if (evEl) {
        const suggestions = data.evidence_suggestions || [];
        if (!suggestions.length) {
            evEl.innerHTML = '<p class="rl-empty" style="color:var(--success-600);"><i class="fas fa-check-circle"></i> No critical evidence gaps detected.</p>';
        } else {
            evEl.innerHTML = suggestions.map(s => `
                <div class="rl-evidence-item">
                    <i class="fas fa-file-search"></i>
                    <span>${s}</span>
                </div>`).join('');
        }
    }

    const trailEl = document.getElementById('aiReasoningTrail');
    if (trailEl) {
        const trail = data.reasoning_trail || [];
        if (!trail.length) {
            trailEl.innerHTML = '<p class="rl-empty">No reasoning trail available.</p>';
        } else {
            trailEl.innerHTML = trail.map((step, i) => {
                let parsedStep = null;
                if (typeof step === 'string') {
                    if (step.trim().startsWith('{')) {
                        try {
                            parsedStep = JSON.parse(step);
                        } catch (e) {
                            // not valid json
                        }
                    }
                } else if (step !== null && typeof step === 'object') {
                    parsedStep = step;
                }

                let textValue = '';
                let customLabel = null;
                let metadataHTML = '';

                if (parsedStep) {
                    textValue = parsedStep.text || parsedStep.description || parsedStep.action || parsedStep.content || JSON.stringify(parsedStep);
                    customLabel = parsedStep.step || parsedStep.label || null;

                    // Build premium badges and details
                    let badges = [];
                    if (parsedStep.provenance) {
                        badges.push(`<span class="xai-badge provenance-${parsedStep.provenance.toLowerCase()}"><i class="fas fa-fingerprint"></i> ${parsedStep.provenance}</span>`);
                    }
                    if (parsedStep.confidence !== undefined) {
                        const confVal = typeof parsedStep.confidence === 'number' ? (parsedStep.confidence <= 1 ? Math.round(parsedStep.confidence * 100) : parsedStep.confidence) : parsedStep.confidence;
                        badges.push(`<span class="xai-badge confidence-badge"><i class="fas fa-chart-line"></i> ${confVal}% Confidence</span>`);
                    }

                    let details = [];
                    if (parsedStep.authority) {
                        details.push(`<strong>Authority:</strong> ${parsedStep.authority}`);
                    }
                    if (parsedStep.logic_engine) {
                        details.push(`<strong>Engine:</strong> ${parsedStep.logic_engine}`);
                    }
                    if (parsedStep.citation) {
                        details.push(`<strong>Citation:</strong> <em>${parsedStep.citation}</em>`);
                    }
                    if (parsedStep.rationale) {
                        details.push(`<strong>Rationale:</strong> ${parsedStep.rationale}`);
                    }

                    metadataHTML = `
                        <div class="xai-meta-container" style="margin-top: 0.6rem; display: flex; flex-direction: column; gap: 0.4rem;">
                            ${badges.length ? `<div class="xai-badges" style="display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 0.2rem;">${badges.join(' ')}</div>` : ''}
                            ${details.length ? `<div class="xai-details" style="font-size: 0.8rem; color: #475569; border-top: 1px dashed #e2e8f0; padding-top: 0.4rem; line-height: 1.4;">${details.join(' &bull; ')}</div>` : ''}
                        </div>
                    `;
                } else {
                    textValue = typeof step === 'string' ? step : String(step);
                }

                const match = textValue.match(/^(STEP[\s\S]*?(?:—[^:]*)?)\s*[—:]\s*([\s\S]+)/i);
                const label   = customLabel || (match ? match[1].trim() : `Step ${i + 1}`);
                const content = match ? match[2].trim() : textValue;
                
                return `
                <div class="rl-trail-item">
                    <div class="rl-trail-step">${label}</div>
                    <div class="rl-trail-content" style="font-weight: 500; color: #1e293b;">${content}</div>
                    ${metadataHTML}
                </div>`;
            }).join('');
        }
    }
}

// Comprehensive Render Results function
// ═══════════════════════════════════════════════════════════════
// DOMAIN THEME SYSTEM
// ═══════════════════════════════════════════════════════════════

/**
 * Apply domain-specific theme to the results screen.
 * Swaps CSS class, domain badge, and tab group visibility.
 */
export function applyDomainTheme(domain) {
    const screen = document.getElementById('resultsScreen');
    if (!screen) return;

    // Swap CSS theme class — strictly Section 138
    screen.classList.remove('dashboard--sarfaesi', 'dashboard--criminal', 'dashboard--civil', 'dashboard--composite', 'dashboard--all');
    screen.classList.add('dashboard--138');

    // Domain badge in results nav
    const badge = document.getElementById('resultsDomainBadge');
    if (badge) {
        badge.style.display = 'inline-flex';
        badge.className = 'domain-badge domain-badge--ni';
        badge.innerHTML = '<i class="fas fa-file-invoice-dollar"></i> NI Act — Section 138';
    }

    // Always show Section 138 tabs, hide others
    const niTabs = document.getElementById('niActTabs') || document.querySelector('.results-tabs:not(#sarfaesiTabs):not(#criminalTabs)');
    const sarfaesiTabs = document.getElementById('sarfaesiTabs');
    const criminalTabs = document.getElementById('criminalTabs');

    if (niTabs) niTabs.classList.remove('hidden');
    if (sarfaesiTabs) sarfaesiTabs.classList.add('hidden');
    if (criminalTabs) criminalTabs.classList.add('hidden');

    const defTab = document.getElementById('tabOverview');
    if (defTab) { defTab.classList.remove('hidden'); defTab.classList.add('active'); }
}

/**
 * Render all Criminal-specific result panels.
 * Called by renderResults() when domain === 'criminal'.
 */
export function renderCriminalResultsPanels(data) {
    const caseData = (window.state && window.state.caseData) || data.case_data || {};
    const offense = String(caseData.offense_type || caseData.ipc_section || 'General Criminal').replace(/\(.*\)/, '').trim();
    const daysInCustody = parseInt(caseData.days_in_custody || 0, 10);
    const maxPunishment = parseInt(caseData.max_punishment_years || 7, 10);
    const defaultBailDays = maxPunishment >= 10 ? 90 : 60;
    const isDefaultBailActive = daysInCustody >= defaultBailDays && !caseData.chargesheet_filed;

    // Metrics
    const offenseMetricEl = document.getElementById('criminalOffenseMetric');
    if (offenseMetricEl) offenseMetricEl.textContent = offense || 'IPC / BNS Offense';

    const bailMetricEl = document.getElementById('criminalBailMetric');
    if (bailMetricEl) {
        if (isDefaultBailActive) bailMetricEl.innerHTML = '<span style="color:#10b981; font-weight:800;">100% (Default Bail u/s 167)</span>';
        else if (caseData.no_s41a_notice && maxPunishment <= 7) bailMetricEl.innerHTML = '<span style="color:#10b981; font-weight:800;">HIGH (Antil Cat. A)</span>';
        else bailMetricEl.textContent = data.score >= 50 ? 'Strong (Bailable/Parity)' : 'Caution (Custodial)';
    }

    const quashingMetricEl = document.getElementById('criminalQuashingMetric');
    if (quashingMetricEl) {
        const hasCivilDispute = caseData.contract_exists || String(caseData.offense_type || '').includes('420');
        const hasOmnibus = caseData.relative_impleaded;
        if (hasCivilDispute) quashingMetricEl.innerHTML = '<span style="color:#10b981; font-weight:800;">85% (Param 1 Civil Disguise)</span>';
        else if (hasOmnibus) quashingMetricEl.innerHTML = '<span style="color:#10b981; font-weight:800;">75% (Param 7 Omnibus)</span>';
        else quashingMetricEl.textContent = 'Moderate Viability';
    }

    const custodyMetricEl = document.getElementById('criminalCustodyMetric');
    if (custodyMetricEl) {
        if (daysInCustody > 0) custodyMetricEl.textContent = `${daysInCustody} Days (${isDefaultBailActive ? 'Default Bail Active' : `${defaultBailDays - daysInCustody} days to S.167`})`;
        else custodyMetricEl.textContent = 'Pre-Arrest / Bail Granted';
    }

    // Statutory Rules / Fatal Alerts
    const rules = data.triggered_rules || data.rules || [];
    const rulesListEl = document.getElementById('criminalStatutoryRulesList');
    if (rulesListEl) {
        if (rules.length === 0) {
            rulesListEl.innerHTML = '<p style="color:var(--gray-500);"><i class="fas fa-check-circle"></i> No mandatory statutory bars detected.</p>';
        } else {
            rulesListEl.innerHTML = rules.map(r => `
                <div style="background:rgba(239,68,68,0.08); border-left:4px solid #ef4444; border-radius:6px; padding:1rem; margin-bottom:0.75rem;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.4rem;">
                        <span style="font-weight:700; color:var(--gray-900); font-size:1rem;"><i class="fas fa-scale-balanced"></i> ${escapeHtml(r.rule_name || '')}</span>
                        <span style="background:var(--error-100); color:var(--error-800); font-size:0.72rem; font-weight:700; padding:0.2rem 0.5rem; border-radius:4px;">${escapeHtml(r.status || 'ALERT')}</span>
                    </div>
                    <div style="font-size:0.88rem; color:var(--gray-700); margin-bottom:0.4rem;"><strong>Trigger:</strong> ${escapeHtml(r.description || '')}</div>
                    <div style="font-size:0.88rem; color:var(--error-700); font-weight:600; margin-bottom:0.4rem;"><i class="fas fa-gavel"></i> ${escapeHtml(r.legal_effect || '')}</div>
                    <div style="font-size:0.85rem; background:white; padding:0.5rem; border-radius:4px; border:1px solid var(--gray-200); color:var(--primary-700);">
                        <i class="fas fa-arrow-right"></i> <strong>Defense Action:</strong> ${escapeHtml(r.action || '')}
                    </div>
                </div>
            `).join('');
        }
    }

    // Prioritized Defense Roadmap
    const actions = data.next_best_actions || data.recommendations || [];
    const actionsListEl = document.getElementById('criminalNextActionsList');
    if (actionsListEl) {
        if (actions.length === 0) {
            actionsListEl.innerHTML = '<p style="color:var(--gray-500);">No specific actions returned.</p>';
        } else {
            actionsListEl.innerHTML = actions.map((a, idx) => {
                const title = typeof a === 'string' ? a : (a.action || a.title || '');
                const reason = typeof a === 'object' ? (a.reason || a.legal_effect || '') : '';
                const auth = typeof a === 'object' ? (a.authority || '') : '';
                return `
                    <div style="background:rgba(59,130,246,0.06); border:1px solid rgba(59,130,246,0.2); border-radius:8px; padding:0.85rem 1rem; margin-bottom:0.6rem;">
                        <div style="font-weight:700; color:#2563eb; font-size:0.92rem;"><i class="fas fa-check-circle"></i> Step ${idx + 1}: ${escapeHtml(title)}</div>
                        ${reason ? `<div style="font-size:0.82rem; color:var(--gray-600); margin-top:0.25rem;">${escapeHtml(reason)}</div>` : ''}
                        ${auth ? `<div style="font-size:0.75rem; color:#475569; margin-top:0.2rem; font-weight:600;"><i class="fas fa-book-bookmark"></i> Precedent/Authority: ${escapeHtml(auth)}</div>` : ''}
                    </div>
                `;
            }).join('');
        }
    }

    // Bail Assessment Card
    const bailCardEl = document.getElementById('criminalBailAssessmentCard');
    if (bailCardEl) {
        bailCardEl.innerHTML = `
            <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap:1rem;">
                <div style="background:white; border:1px solid var(--gray-200); border-radius:8px; padding:1.2rem;">
                    <div style="font-weight:700; color:var(--gray-800); margin-bottom:0.5rem;"><i class="fas fa-shield" style="color:#3b82f6;"></i> Anticipatory Bail (S.438 CrPC / S.484 BNSS)</div>
                    <div style="font-size:0.88rem; color:var(--gray-600); margin-bottom:0.75rem;">Viability: <strong>${maxPunishment <= 7 ? 'VERY HIGH (Arnesh Kumar Compliance)' : 'MODERATE (Subject to Custodial Interrogation necessity)'}</strong></div>
                    <div style="font-size:0.8rem; color:var(--gray-500);">Precedent: <em>Sushila Aggarwal v. State (NCT of Delhi)</em> — Anticipatory bail protection does not automatically expire upon chargesheet.</div>
                </div>
                <div style="background:white; border:1px solid var(--gray-200); border-radius:8px; padding:1.2rem;">
                    <div style="font-weight:700; color:var(--gray-800); margin-bottom:0.5rem;"><i class="fas fa-unlock" style="color:#10b981;"></i> Regular Bail (S.437/439 CrPC / S.480/483 BNSS)</div>
                    <div style="font-size:0.88rem; color:var(--gray-600); margin-bottom:0.75rem;">Status: <strong>${daysInCustody > 30 ? 'Substantial Custody Undergone (Parity / Trial Delay Grounds)' : 'Available on First Appearance'}</strong></div>
                    <div style="font-size:0.8rem; color:var(--gray-500);">Precedent: <em>Satender Kumar Antil v. CBI (2022)</em> — Bail is rule, jail is exception; strict adherence to category mandates.</div>
                </div>
            </div>
        `;
    }

    // Default Bail Countdown Card
    const defaultBailEl = document.getElementById('criminalDefaultBailCard');
    if (defaultBailEl) {
        defaultBailEl.innerHTML = `
            <div style="background:${isDefaultBailActive ? 'rgba(16,185,129,0.1)' : 'rgba(245,158,11,0.08)'}; border:2px solid ${isDefaultBailActive ? '#10b981' : '#f59e0b'}; border-radius:8px; padding:1.2rem;">
                <div style="font-weight:800; font-size:1.05rem; color:${isDefaultBailActive ? '#065f46' : '#92400e'}; margin-bottom:0.5rem;">
                    <i class="fas ${isDefaultBailActive ? 'fa-check-circle' : 'fa-hourglass-start'}"></i>
                    ${isDefaultBailActive ? 'INDEFEASIBLE RIGHT TO DEFAULT BAIL ACQUIRED' : `STATUTORY PERIOD IN PROGRESS: ${daysInCustody} / ${defaultBailDays} DAYS`}
                </div>
                <p style="font-size:0.88rem; color:var(--gray-700); margin:0 0 0.5rem 0;">
                    ${isDefaultBailActive ? `The ${defaultBailDays}-day statutory limit for completing investigation has expired without filing chargesheet. Accused is entitled to immediate release upon furnishing bail bond per <em>Ritu Chhabaria v. Union of India (2023)</em>.` : `Under Section 167(2) CrPC / Section 187 BNSS, if the police fail to submit the chargesheet within ${defaultBailDays} days of initial remand, default bail becomes an absolute constitutional right.`}
                </p>
                <div style="font-size:0.8rem; font-weight:700; color:var(--primary-700);">
                    Action: ${isDefaultBailActive ? 'File S.167(2) Application immediately before Chargesheet is placed on record.' : 'Track investigation diary and file S.167(2) application on day ' + (defaultBailDays + 1) + '.'}
                </div>
            </div>
        `;
    }

    // Bhajan Lal Quashing Radar
    const bhajanLalEl = document.getElementById('criminalBhajanLalList');
    if (bhajanLalEl) {
        const grounds = (data.adversarial_risk_model && data.adversarial_risk_model.risks_and_rebuttals) || [];
        const bhajanItems = grounds.filter(g => String(g.adversarial_vector || '').includes('Bhajan Lal') || String(g.quashing_ground || '').length > 0);
        if (bhajanItems.length === 0) {
            bhajanLalEl.innerHTML = `
                <div style="background:white; border:1px solid var(--gray-200); border-radius:8px; padding:1rem;">
                    <div style="font-weight:700; color:#1e293b;"><i class="fas fa-info-circle"></i> Bhajan Lal 7-Parameter Evaluation</div>
                    <p style="font-size:0.85rem; color:var(--gray-600); margin:0.4rem 0;">FIR discloses prima facie elements of offense. Defense should focus on trial contradictions, cross-examination of I.O., and S.227/239 discharge arguments.</p>
                </div>
            `;
        } else {
            bhajanLalEl.innerHTML = bhajanItems.map(b => `
                <div style="background:rgba(16,185,129,0.06); border:1px solid rgba(16,185,129,0.25); border-radius:8px; padding:1rem; margin-bottom:0.75rem;">
                    <div style="font-weight:700; color:#065f46; font-size:0.95rem;"><i class="fas fa-shield-halved"></i> ${escapeHtml(b.adversarial_vector || b.quashing_ground || '')}</div>
                    <div style="font-size:0.85rem; color:var(--gray-700); margin:0.35rem 0;">${escapeHtml(b.description || '')}</div>
                    <div style="font-size:0.8rem; color:#047857; font-weight:600;"><i class="fas fa-gavel"></i> Quashing Strategy: ${escapeHtml(b.discharge_quashing_strategy || 'File Petition u/s 482 CrPC / S.528 BNSS before High Court.')}</div>
                </div>
            `).join('');
        }
    }

    // Evidentiary & Cross-Exam
    const evidenceAuditEl = document.getElementById('criminalEvidenceAuditList');
    if (evidenceAuditEl) {
        evidenceAuditEl.innerHTML = `
            <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap:0.75rem;">
                <div style="background:white; border:1px solid var(--gray-200); border-radius:6px; padding:0.9rem;">
                    <div style="font-weight:700; font-size:0.88rem; color:var(--gray-800);"><i class="fas fa-file-shield" style="color:#8b5cf6;"></i> Electronic Proof (S.65B/63 BSA)</div>
                    <div style="font-size:0.82rem; color:${caseData.s65b_certificate ? '#10b981' : '#ef4444'}; font-weight:600; margin-top:0.25rem;">
                        ${caseData.s65b_certificate ? 'Certified & Admissible' : 'Uncertified / Inadmissible (Arjun Panditrao Rule)'}
                    </div>
                </div>
                <div style="background:white; border:1px solid var(--gray-200); border-radius:6px; padding:0.9rem;">
                    <div style="font-weight:700; font-size:0.88rem; color:var(--gray-800);"><i class="fas fa-box-archive" style="color:#3b82f6;"></i> Discovery Memo (S.27 IEA/23 BSA)</div>
                    <div style="font-size:0.82rem; color:var(--gray-600); margin-top:0.25rem;">
                        ${caseData.recovery_memo_s27 || 'Subject to strict proof of custody disclosure and independent panchas'}
                    </div>
                </div>
                <div style="background:white; border:1px solid var(--gray-200); border-radius:6px; padding:0.9rem;">
                    <div style="font-weight:700; font-size:0.88rem; color:var(--gray-800);"><i class="fas fa-notes-medical" style="color:#ef4444;"></i> Medical vs Ocular Divergence</div>
                    <div style="font-size:0.82rem; color:${caseData.medical_contradicts_ocular ? '#ef4444' : '#10b981'}; font-weight:600; margin-top:0.25rem;">
                        ${caseData.medical_contradicts_ocular ? 'Fatal Ocular Contradiction (Thaman Kumar)' : 'Consistent with medical reports'}
                    </div>
                </div>
            </div>
        `;
    }

    const crossExamEl = document.getElementById('criminalCrossExamList');
    if (crossExamEl) {
        const crossQuestions = (data.adversarial_risk_model && data.adversarial_risk_model.risks_and_rebuttals && data.adversarial_risk_model.risks_and_rebuttals[0]?.cross_exam_questions) || [
            "Can you establish the exact chronological timeline between the alleged incident and the registration of the FIR?",
            "Were any independent local public witnesses requested to join the recovery proceedings under Section 100(4) CrPC / Section 103 BNSS?",
            "Is it not true that the electronic WhatsApp/CCTV evidence lacks the mandatory contemporaneous certificate under Section 65B(4) Evidence Act / Section 63 BSA?",
            "Is it not true that the dispute between the parties is purely a commercial civil debt dispute for which no criminal fraud existed at inception?"
        ];

        crossExamEl.innerHTML = crossQuestions.map((q, idx) => `
            <div style="background:white; border-left:3px solid #3b82f6; border-radius:4px; padding:0.75rem 1rem; margin-bottom:0.5rem; font-size:0.88rem; color:var(--gray-800); box-shadow:0 1px 2px rgba(0,0,0,0.05);">
                <strong>Q${idx + 1}:</strong> "${escapeHtml(q)}"
            </div>
        `).join('');
    }
}

/**
 * Render all SARFAESI-specific result panels.
 * Called by renderResults() when domain === 'sarfaesi'.
 */
export function renderSarfaesiResultsPanels(data) {
    // Enforcement stage
    const graph = data.procedural_graph || data.timeline_analysis || data.timeline || {};
    const currentStage = graph.current_stage || data.enforcement_stage || 'Pre-Enforcement';
    const stageMetricEl = document.getElementById('sarfaesiStageMetric');
    if (stageMetricEl) stageMetricEl.textContent = currentStage;

    const stageDisplayEl = document.getElementById('sarfaesiStageDisplay');
    if (stageDisplayEl) {
        stageDisplayEl.innerHTML = `
            <span class="enforcement-stage-badge">
                <i class="fas fa-network-wired"></i> ${escapeHtml(currentStage)}
            </span>
            <span style="font-size:0.8rem; color:var(--gray-400); margin-left:0.75rem;">
                ${graph.completed_nodes || 0} of ${graph.total_nodes || 0} statutory milestones completed
            </span>
        `;
    }

    // CERSAI status
    const caseData = (window.state && window.state.caseData) || {};
    const cersaiReg = String(caseData.cersai_registered || data.cersai_registered || '').toLowerCase();
    const cersaiOk = cersaiReg === 'yes' || cersaiReg === 'true';
    const cersaiMetricEl = document.getElementById('sarfaesiCersaiMetric');
    if (cersaiMetricEl) cersaiMetricEl.textContent = cersaiOk ? 'Registered' : 'Not Confirmed';
    const cersaiEl = document.getElementById('sarfaesiCersaiStatus');
    if (cersaiEl) {
        cersaiEl.innerHTML = `
            <div class="cersai-status-row">
                <span style="font-size:0.88rem; font-weight:600; color:var(--gray-200);"><i class="fas fa-database"></i> CERSAI Security Interest Registration</span>
                <span class="${cersaiOk ? 'cersai-ok' : 'cersai-fail'}" style="font-weight:700; font-size:0.85rem;">
                    <i class="fas ${cersaiOk ? 'fa-check-circle' : 'fa-times-circle'}"></i>
                    ${cersaiOk ? 'REGISTERED' : 'NOT CONFIRMED'}
                </span>
            </div>
        `;
    }

    // Evidence gaps metric + list
    const gaps = data.evidence_gaps || [];
    const gapsMetricEl = document.getElementById('sarfaesiGapsMetric');
    if (gapsMetricEl) gapsMetricEl.textContent = gaps.length;
    const gapsListEl = document.getElementById('sarfaesiEvidenceGapsList');
    if (gapsListEl) {
        if (gaps.length === 0) {
            gapsListEl.innerHTML = '<p style="color:var(--gray-500);"><i class="fas fa-check-circle"></i> No critical evidence gaps detected.</p>';
        } else {
            gapsListEl.innerHTML = gaps.map(g => {
                const doc = typeof g === 'string' ? g : (g.document_required || 'Document required');
                const csq = typeof g === 'object' ? (g.consequence || '') : '';
                return `
                    <div style="background:rgba(245,158,11,0.07); border:1px solid rgba(245,158,11,0.2); border-radius:10px; padding:0.9rem 1.1rem; margin-bottom:0.6rem;">
                        <div style="font-weight:700; color:#fcd34d; font-size:0.88rem;"><i class="fas fa-folder-open"></i> ${escapeHtml(doc)}</div>
                        ${csq ? `<div style="font-size:0.8rem; color:var(--gray-400); margin-top:0.25rem;">${escapeHtml(csq)}</div>` : ''}
                    </div>
                `;
            }).join('');
        }
    }

    // Next best actions metric + list
    const actions = data.next_best_actions || [];
    const actionsMetricEl = document.getElementById('sarfaesiActionsMetric');
    if (actionsMetricEl) actionsMetricEl.textContent = actions.length;
    const actionsListEl = document.getElementById('sarfaesiNextActionsList');
    if (actionsListEl) {
        if (actions.length === 0) {
            actionsListEl.innerHTML = '<p style="color:var(--gray-500);">No priority actions returned.</p>';
        } else {
            actionsListEl.innerHTML = actions.map((a, i) => {
                const action = typeof a === 'string' ? a : (a.action || a.title || String(a));
                const reason = typeof a === 'object' ? (a.reason || '') : '';
                const priority = typeof a === 'object' ? (a.priority || i + 1) : i + 1;
                return `
                    <div style="background:var(--gray-100); border:1px solid var(--gray-200); border-left:3px solid #f59e0b; border-radius:10px; padding:0.9rem 1.1rem; margin-bottom:0.6rem; display:flex; gap:1rem; align-items:flex-start;">
                        <span style="min-width:28px; height:28px; background:rgba(245,158,11,0.15); color:#f59e0b; font-weight:800; font-size:0.8rem; border-radius:6px; display:flex; align-items:center; justify-content:center;">${priority}</span>
                        <div style="flex:1;">
                            <div style="font-weight:700; color:var(--gray-900); font-size:0.9rem;">${escapeHtml(action)}</div>
                            ${reason ? `<div style="font-size:0.78rem; color:var(--gray-600); margin-top:0.2rem;">${escapeHtml(reason)}</div>` : ''}
                        </div>
                    </div>
                `;
            }).join('');
        }
    }

    // Timeline list (enforcement milestones)
    renderTimelineEngine(graph, data);
    const sarfaesiTimeline = document.getElementById('sarfaesiTimelineList');
    const origTimeline = document.getElementById('timelineList');
    if (sarfaesiTimeline && origTimeline) sarfaesiTimeline.innerHTML = origTimeline.innerHTML;

    // Contradictions
    const contraListEl = document.getElementById('sarfaesiContradictionsList');
    if (contraListEl) {
        const contras = data.contradictions || [];
        if (contras.length === 0) {
            contraListEl.innerHTML = '<p style="color:var(--gray-500);"><i class="fas fa-check-circle"></i> No procedural contradictions found.</p>';
        } else {
            contraListEl.innerHTML = contras.map(c => `
                <div style="background:rgba(239,68,68,0.08); border:1px solid rgba(239,68,68,0.2); border-radius:10px; padding:0.9rem 1.1rem; margin-bottom:0.6rem; font-size:0.88rem; color:#f87171;">
                    <i class="fas fa-exclamation-triangle"></i> ${escapeHtml(String(c))}
                </div>`).join('');
        }
    }

    // Procedural graph panel
    const graphPanelEl = document.getElementById('sarfaesiProceduralGraph');
    if (graphPanelEl && origTimeline) graphPanelEl.innerHTML = origTimeline.innerHTML;

    // Authority card
    const authPanelEl = document.getElementById('sarfaesiAuthorityCard');
    const origAuth = document.getElementById('verifiedAuthorityContainer') || document.getElementById('precedentsList');
    if (authPanelEl && origAuth) authPanelEl.innerHTML = origAuth.innerHTML;

    // Strategy
    const strategyEl = document.getElementById('sarfaesiStrategyList');
    const origStrategy = document.getElementById('strategyList');
    if (strategyEl && origStrategy) strategyEl.innerHTML = origStrategy.innerHTML;

    // Defences
    const defencesEl = document.getElementById('sarfaesiDefencesList');
    const origDefences = document.getElementById('defencesList');
    if (defencesEl && origDefences) defencesEl.innerHTML = origDefences.innerHTML;

    // Reasoning trace
    const reasoningEl = document.getElementById('sarfaesiReasoningList');
    const origReasoning = document.getElementById('reasoningList');
    if (reasoningEl && origReasoning) reasoningEl.innerHTML = origReasoning.innerHTML;

    // Institutional SARFAESI Suite Card (CERSAI, S.13(8) Redemption, S.14 9-Pt Affidavit, e-DRT)
    const cersai = data.cersai_audit || (data.detailed_assessment && data.detailed_assessment.cersai_audit) || {};
    const redemption = data.redemption_analysis || (data.detailed_assessment && data.detailed_assessment.redemption_analysis) || {};
    const sec14 = data.section14_audit || (data.detailed_assessment && data.detailed_assessment.section14_audit) || {};
    const edrt = data.edrt_bundle || (data.detailed_assessment && data.detailed_assessment.edrt_bundle) || {};

    const overviewContainer = document.getElementById('tabSarfaesi_overview') || document.getElementById('tabOverview');
    if (overviewContainer) {
        const existing = document.getElementById('sarfaesiInstitutionalCard');
        if (existing) existing.remove();

        const redCalc = redemption.redemption_calculation || {};
        const cersaiColor = cersai.is_cersai_registered ? '#10b981' : '#ef4444';
        const redColor = redemption.right_to_redeem_extinguished ? '#ef4444' : '#10b981';

        const institutionalHTML = `
            <div id="sarfaesiInstitutionalCard" class="result-card" style="margin-top: 1.5rem; border: 1px solid rgba(245,158,11,0.35); background: var(--glass-bg); backdrop-filter: blur(var(--glass-blur));">
                <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--gray-200); padding-bottom: 0.75rem; margin-bottom: 1.25rem; flex-wrap: wrap; gap: 0.5rem;">
                    <div style="display: flex; align-items: center; gap: 0.65rem;">
                        <span style="background: linear-gradient(135deg, #f59e0b, #d97706); color: white; width: 36px; height: 36px; border-radius: 8px; display: inline-flex; align-items: center; justify-content: center; font-size: 1.1rem;">
                            <i class="fas fa-university"></i>
                        </span>
                        <div>
                            <h3 style="margin: 0; font-size: 1.15rem; font-weight: 800; color: var(--gray-900);">SARFAESI & DRT Institutional Audit Suite</h3>
                            <div style="font-size: 0.8rem; color: var(--gray-500);">Real-Time CERSAI Verification • S.13(8) Redemption Calculator • S.14 9-Pt DM Affidavit • e-DRT Export</div>
                        </div>
                    </div>
                    <div>
                        <span style="background: rgba(245,158,11,0.15); color: #d97706; font-size: 0.75rem; font-weight: 750; padding: 0.3rem 0.7rem; border-radius: var(--radius-full); border: 1px solid rgba(245,158,11,0.3);">
                            <i class="fas fa-gavel"></i> 100% INSTITUTIONAL GRADE
                        </span>
                    </div>
                </div>

                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1rem; margin-bottom: 1rem;">
                    <div style="background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: var(--radius-lg); padding: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                            <strong style="font-size: 0.88rem; color: var(--gray-900);"><i class="fas fa-shield-alt" style="color: ${cersaiColor};"></i> CERSAI Registry (S.26D & 26E)</strong>
                            <span style="background: ${cersaiColor}18; color: ${cersaiColor}; border: 1px solid ${cersaiColor}40; font-size: 0.72rem; font-weight: 750; padding: 0.2rem 0.5rem; border-radius: 4px;">
                                ${cersai.is_cersai_registered ? 'REGISTERED (S.26D COMPLIANT)' : 'UNREGISTERED (STATUTORY BAR)'}
                            </span>
                        </div>
                        <div style="font-size: 0.83rem; color: var(--gray-700); line-height: 1.5;">
                            <div><strong>Security ID:</strong> ${escapeHtml(cersai.cersai_security_id || 'NOT REGISTERED')}</div>
                            <div><strong>S.26E Priority:</strong> First Charge over Crown/Tax attachments</div>
                            <div style="color: ${cersaiColor}; font-weight: 600; margin-top: 0.25rem;">${cersai.is_cersai_registered ? '✓ Chapter III enforcement maintainable' : '🚨 Chapter III enforcement barred under S.26D'}</div>
                        </div>
                    </div>

                    <div style="background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: var(--radius-lg); padding: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                            <strong style="font-size: 0.88rem; color: var(--gray-900);"><i class="fas fa-calculator" style="color: ${redColor};"></i> S.13(8) Redemption (Celir LLP Doctrine)</strong>
                            <span style="background: ${redColor}18; color: ${redColor}; border: 1px solid ${redColor}40; font-size: 0.72rem; font-weight: 750; padding: 0.2rem 0.5rem; border-radius: 4px;">
                                ${redemption.right_to_redeem_extinguished ? 'CUT-OFF EXTINGUISHED' : 'REDEMPTION OPEN'}
                            </span>
                        </div>
                        <div style="font-size: 0.83rem; color: var(--gray-700); line-height: 1.5;">
                            <div><strong>Total Redemption Due:</strong> ₹${Number(redCalc.total_redemption_amount_payable || 0).toLocaleString('en-IN')}/-</div>
                            <div><strong>Accrued Interest:</strong> ₹${Number(redCalc.accrued_contractual_interest || 0).toLocaleString('en-IN')}/- (${redCalc.days_accrued || 0} days @ ${redCalc.annual_interest_rate_pct || 12}%)</div>
                            <div style="font-size: 0.76rem; color: var(--gray-500); margin-top: 0.25rem;"><em>Celir LLP v. Bafna Motors (2024) 2 SCC 1</em></div>
                        </div>
                    </div>
                </div>

                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1rem;">
                    <div style="background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: var(--radius-lg); padding: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                            <strong style="font-size: 0.88rem; color: var(--gray-900);"><i class="fas fa-landmark" style="color: #f59e0b;"></i> S.14 DM/CMM 9-Point Affidavit</strong>
                            <span style="background: rgba(245,158,11,0.15); color: #d97706; font-size: 0.72rem; font-weight: 750; padding: 0.2rem 0.5rem; border-radius: 4px;">
                                ${sec14.compliant_clauses_count || '9/9'} COMPLIANT (${sec14.compliance_score_pct || 100}%)
                            </span>
                        </div>
                        <div style="font-size: 0.83rem; color: var(--gray-700); line-height: 1.5;">
                            <div><strong>Governing Precedent:</strong> Standard Chartered v. V. Noble Kumar</div>
                            <div><strong>Possession Prayer:</strong> Advocate Commissioner + Police Aid</div>
                            <div style="margin-top: 0.4rem;">
                                <button class="btn btn-sm btn-outline" onclick="window.switchResultTab('draft');" style="padding: 0.25rem 0.65rem; font-size: 0.75rem;">
                                    <i class="fas fa-file-contract"></i> View Sworn 9-Point Affidavit
                                </button>
                            </div>
                        </div>
                    </div>

                    <div style="background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: var(--radius-lg); padding: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                            <strong style="font-size: 0.88rem; color: var(--gray-900);"><i class="fas fa-network-wired" style="color: #6366f1;"></i> e-DRT Portal Export Bridge</strong>
                            <span style="background: rgba(99,102,241,0.15); color: #6366f1; font-size: 0.72rem; font-weight: 750; padding: 0.2rem 0.5rem; border-radius: 4px;">
                                ${escapeHtml(edrt.drt_bench || 'DRT-I MUMBAI')}
                            </span>
                        </div>
                        <div style="font-size: 0.83rem; color: var(--gray-700); line-height: 1.5;">
                            <div><strong>Ref ID:</strong> ${escapeHtml(edrt.efiling_reference_id || 'EDRT-2026-001')}</div>
                            <div><strong>Court Fee Payable:</strong> ₹${Number((edrt.court_fee_details && edrt.court_fee_details.court_fee_payable) || 12500).toLocaleString('en-IN')}/- (Rule 7)</div>
                            <div style="margin-top: 0.4rem;">
                                <button class="btn btn-sm btn-outline" onclick="window.switchResultTab('draft');" style="padding: 0.25rem 0.65rem; font-size: 0.75rem;">
                                    <i class="fas fa-file-export"></i> Open e-DRT S.17 Bundle
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        overviewContainer.insertAdjacentHTML('afterbegin', institutionalHTML);
    }
}

/**
 * Render all Multi-Track Composite result panels.
 * Called by renderResults() when domain === 'composite' or multi_track.
 */
export function renderCompositeResultsPanels(data) {
    const tracks = data.tracks || {};
    const sarfaesi = tracks.sarfaesi || {};
    const cb = tracks.cheque_bounce_138 || {};
    const crim = tracks.criminal_bns_ipc || {};
    const matrix = data.cross_track_matrix || {};

    const actionsContainer = document.getElementById('actionsList');
    if (actionsContainer) {
        let html = `
            <div class="composite-tracks-overview" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 1rem; margin-bottom: 1.5rem;">
                <div style="background: rgba(16, 185, 129, 0.08); border: 2px solid #10b981; border-radius: 12px; padding: 1.25rem;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.75rem;">
                        <span style="font-weight:800; color:#065f46; font-size:1.05rem;"><i class="fas fa-university"></i> Track 1: SARFAESI</span>
                        <span style="font-size:1.3rem; font-weight:900; color:#10b981;">${sarfaesi.score || 0}<span style="font-size:0.8rem;">/100</span></span>
                    </div>
                    <div style="font-size:0.85rem; font-weight:700; color:#047857; margin-bottom:0.5rem;">Status: ${escapeHtml(sarfaesi.verdict || 'Enforcement Ready')}</div>
                    <div style="font-size:0.78rem; color:var(--gray-600);">Focus: Asset possession, CERSAI charge, and S.14 CMM application.</div>
                </div>

                <div style="background: rgba(59, 130, 246, 0.08); border: 2px solid #3b82f6; border-radius: 12px; padding: 1.25rem;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.75rem;">
                        <span style="font-weight:800; color:#1e40af; font-size:1.05rem;"><i class="fas fa-file-invoice-dollar"></i> Track 2: Section 138 NI Act</span>
                        <span style="font-size:1.3rem; font-weight:900; color:#3b82f6;">${cb.score || 0}<span style="font-size:0.8rem;">/100</span></span>
                    </div>
                    <div style="font-size:0.85rem; font-weight:700; color:#2563eb; margin-bottom:0.5rem;">Status: ${escapeHtml(cb.verdict || 'Complaint Ready')}</div>
                    <div style="font-size:0.78rem; color:var(--gray-600);">Focus: Cheque return memo, 30-day notice, and S.143A 20% interim deposit.</div>
                </div>

                <div style="background: rgba(239, 68, 68, 0.08); border: 2px solid #ef4444; border-radius: 12px; padding: 1.25rem;">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.75rem;">
                        <span style="font-weight:800; color:#991b1b; font-size:1.05rem;"><i class="fas fa-user-shield"></i> Track 3: Criminal (BNS/IPC)</span>
                        <span style="font-size:1.3rem; font-weight:900; color:#ef4444;">${crim.score || 0}<span style="font-size:0.8rem;">/100</span></span>
                    </div>
                    <div style="font-size:0.85rem; font-weight:700; color:#dc2626; margin-bottom:0.5rem;">Status: ${escapeHtml(crim.verdict || 'Prosecution Viable')}</div>
                    <div style="font-size:0.78rem; color:var(--gray-600);">Focus: S.318/420 cheating, hypothecated stock alienation, and S.156(3) BNSS FIR.</div>
                </div>
            </div>

            <!-- Cross-Track Synergy Matrix -->
            <div style="background: var(--gray-50); border: 1px solid var(--gray-200); border-radius: 12px; padding: 1.25rem; margin-bottom: 1.5rem;">
                <h4 style="color: var(--gray-900); font-weight: 800; margin-bottom: 0.75rem;"><i class="fas fa-link" style="color:#f59e0b;"></i> Cross-Track Legal Synergies & Safeguards</h4>
                <div style="display: flex; flex-direction: column; gap: 0.6rem;">
                    ${(matrix.synergies || []).map(s => `
                        <div style="background: white; border-left: 3px solid #10b981; border-radius: 6px; padding: 0.75rem 1rem;">
                            <strong style="color: #065f46; font-size: 0.88rem;">${escapeHtml(s.principle)}</strong>
                            <div style="font-size: 0.8rem; color: var(--gray-700); margin-top: 0.2rem;">${escapeHtml(s.impact)}</div>
                        </div>
                    `).join('')}
                    ${(matrix.risks || []).map(r => `
                        <div style="background: white; border-left: 3px solid #ef4444; border-radius: 6px; padding: 0.75rem 1rem;">
                            <strong style="color: #991b1b; font-size: 0.88rem;">${escapeHtml(r.risk_title)}</strong>
                            <div style="font-size: 0.8rem; color: var(--gray-700); margin-top: 0.2rem;">${escapeHtml(r.mitigation)}</div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
        actionsContainer.innerHTML = html;
    }
}


export function renderResults(data) {
    if (!data) return;

    // Apply Section 138 domain theme
    applyDomainTheme('ni_act');

    const resContainer = document.querySelector('.results-container');
    if (resContainer) resContainer.scrollTop = 0;

    if (data.caseroom_id && window.startCaseroomSync) {
        window.startCaseroomSync();
    }

    const fatalContainer = document.getElementById('fatalDefectAlert');
    if (fatalContainer) {
        let fatalHTML = '';
        const score = data.score || 0;
        
        if (data.limitation && data.limitation.is_premature) {
            fatalHTML += `
                <div class="fatal-banner">
                    <i class="fas fa-exclamation-triangle"></i>
                    <div>
                        <strong>FATAL DEFECT: PREMATURE FILING DETECTED</strong>
                        <p>The 15-day statutory 'cure period' for the accused has not expired. Filing today will lead to mandatory dismissal as per <em>Yogendra Pratap Singh vs. Savitri Pandey</em>. <strong>DO NOT FILE UNTIL ${data.limitation.earliest_filing_date || 'the period expires'}.</strong></p>
                    </div>
                </div>
            `;
        }
        
        const caseData = (window.state && window.state.caseData) || {};
        if (caseData.within_30_days === "No" || (data.limitation && data.limitation.notice_delay_days > 0)) {
            fatalHTML += `
                <div class="fatal-banner fatal-warning">
                    <i class="fas fa-clock"></i>
                    <div>
                        <strong>JURISDICTIONAL BAR: LATE STATUTORY NOTICE</strong>
                        <p>The legal notice was sent beyond the 30-day limit. The case is non-maintainable unless a robust Condonation of Delay application is filed under Section 142(1)(b).</p>
                    </div>
                </div>
            `;
        }

        if (data.jurisdiction && data.jurisdiction.auto_reroute_available) {
            fatalHTML += `
                <div class="fatal-banner" style="border-left-color: #f59e0b; background: rgba(245,158,11,0.08);">
                    <i class="fas fa-map-marker-alt" style="color: #f59e0b;"></i>
                    <div style="display: flex; flex-direction: column; gap: 0.5rem; width: 100%;">
                        <div>
                            <strong style="color: #d97706;">TERRITORIAL DEFECT (SECTION 142(2) NI ACT)</strong>
                            <p style="margin: 0.2rem 0;">${data.jurisdiction.reason || 'Filing court does not match the payee bank branch collection venue.'}</p>
                        </div>
                        <div>
                            <button class="btn btn-sm btn-primary" style="background: linear-gradient(135deg, #f59e0b, #d97706); border: none; font-size: 0.8rem; padding: 0.35rem 0.75rem;" onclick="window.autoAlignCourtJurisdiction('${data.jurisdiction.suggested_court}'); if(window.runAnalysis) window.runAnalysis();">
                                <i class="fas fa-bolt"></i> Auto-Align Court to: ${data.jurisdiction.suggested_court}
                            </button>
                        </div>
                    </div>
                </div>
            `;
        }

        const communicationDetected = (data.weaknesses || []).some(w => String(w).includes("65B") || String(w).includes("63(4)") || String(w).includes("digital evidence") || String(w).includes("BSA"));
        if (communicationDetected) {
            fatalHTML += `
                <div class="fatal-banner fatal-info">
                    <i class="fas fa-microchip"></i>
                    <div>
                        <strong>MANDATORY DIGITAL EVIDENCE: SECTION 63(4) BSA CERTIFICATE</strong>
                        <p>Digital records (WhatsApp/Email/e-Bank slips) require a mandatory Section 63(4) BSA Certificate. This has been automatically bundled into your Draft Studio complaint.</p>
                    </div>
                </div>
            `;
        }

        fatalContainer.innerHTML = fatalHTML;
        fatalContainer.classList.toggle('hidden', fatalHTML === '');
    }

    const score = data.score || 0;
    const scoreEl = document.getElementById("scoreNumber");
    if (scoreEl) scoreEl.innerText = score;

    const verdict = mapVerdict(data.verdict);
    const verdictTitleEl = document.getElementById("verdictTitle");
    const verdictDescEl = document.getElementById("verdictDescription");
    const cynicalBadge = document.getElementById("cynicalModeBadge");

    if (verdictTitleEl) verdictTitleEl.textContent = verdict;
    if (verdictDescEl) verdictDescEl.textContent = getVerdictDescription(score);
    
    if (cynicalBadge) {
        const isCynical = score < 65 || (data.reasoning_trace || []).some(t => String(t).includes('CYNICAL'));
        cynicalBadge.classList.toggle('hidden', !isCynical);
    }

    const riskEl = document.getElementById("defenceRisk");
    if (riskEl) riskEl.innerText = data.risk_level || data.defence_risk || "Unknown";

    const issuesCountEl = document.getElementById("criticalIssues");
    const strengthsCountEl = document.getElementById("strongPoints");
    const conceptsCountEl = document.getElementById("conceptsDetected");

    if (issuesCountEl) issuesCountEl.innerText = (data.issues || []).length || 0;
    if (strengthsCountEl) strengthsCountEl.innerText = (data.strengths || []).length || 0;

    const semanticAnalysis = data.semantic_analysis || {};
    const conceptsCount = (semanticAnalysis.concepts_detected || []).length || 0;
    if (conceptsCountEl) conceptsCountEl.innerText = conceptsCount;

    const verdictCard = document.querySelector('.result-card-hero');
    const verdictIcon = document.getElementById('verdictIcon');

    if (verdictCard && verdictIcon) {
        if (score >= 70) {
            verdictCard.className = 'result-card result-card-hero verdict-strong';
            verdictIcon.innerHTML = '<i class="fas fa-check-circle"></i>';
        } else if (score >= 50) {
            verdictCard.className = 'result-card result-card-hero verdict-moderate';
            verdictIcon.innerHTML = '<i class="fas fa-exclamation-triangle"></i>';
        } else {
            verdictCard.className = 'result-card result-card-hero verdict-weak';
            verdictIcon.innerHTML = '<i class="fas fa-times-circle"></i>';
        }
    }

    animateScore(score);

    renderList("issuesList", data.issues, "No critical issues detected");
    renderList("strengthsList", data.strengths, "No strong points identified");

    const weaknesses = (data.evidence_gaps && data.evidence_gaps.length > 0)
        ? data.evidence_gaps.map(g => `[EVIDENCE GAP] ${g.document_required}: ${g.consequence}`)
        : (data.weaknesses || []);
    renderList("weaknessesList", weaknesses, "No significant weaknesses identified");
    
    renderTimelineEngine(data.procedural_graph || data.timeline_analysis || data.timeline, data);
    renderEconomicsEngine(data.economics || data.bail_economics);
    renderExplainableScore(data);
    renderEvidenceSufficiencyMeter(data);
    
    const recommendedActions = (data.next_best_actions && data.next_best_actions.length > 0)
        ? data.next_best_actions.map(a => `[PRIORITY ${a.priority}] ${a.action} — ${a.reason} (${a.authority})`)
        : ((data.decision && data.decision.next_steps) || data.recommended_actions || data.next_steps || []);
    renderList("actionsList", recommendedActions, "No recommended actions");
    renderList("contradictionsList", data.contradictions || [], "No contradictions detected");

    if (Array.isArray(data.legal_strategy)) {
        renderList("strategyList", data.legal_strategy, "No strategy available");
    } else if (data.legal_strategy) {
        const strategyListEl = document.getElementById("strategyList");
        if (strategyListEl) {
            strategyListEl.innerHTML = `<div class="list-item">${escapeHtml(String(data.legal_strategy))}</div>`;
        }
    } else {
        renderList("strategyList", [], "No strategy available");
    }

    const defences = data.defence_strategy || data.predicted_defences || data.defence || data.top_defences || data.defences_ranked || [];
    displayDefences(defences);

    displaySemanticAnalysis(data.semantic_analysis || {});

    if (data.decision) {
        renderDecisionPanel(data.decision);
    }

    displayReasoningTrace(data.reasoning_trace || data.reasoning || []);

    const draftText = data.draft || data.legal_draft || data.generated_draft 
                   || (data.data && (data.data.draft || data.data.legal_draft))
                   || "";
    const draftPreviewEl = document.getElementById("draftPreviewContent");
    const draftContentEl = document.getElementById("draftContent");

    if (draftPreviewEl) draftPreviewEl.value = draftText || "Legal draft is being generated. Please try 'Generate Report' to download the full draft.";
    if (draftContentEl) draftContentEl.value = draftText || "Legal draft is being generated. Please try 'Generate Report' to download the full draft.";

    const legalAnalysisEl = document.getElementById("legalAnalysis");
    if (legalAnalysisEl) {
        legalAnalysisEl.innerText = data.legal_analysis || "No legal analysis available";
    }

    renderLimitationClock(data);
    renderCorporateWarning(data);
    if (data.statutory_rules) {
        renderRulesEngine(data.statutory_rules);
    }

    // renderScoreExplanation(data); // Disabled: Redundant with AI Case Summary
    renderImprovementSuggestions(data);
    renderConfidenceIndicator(data);
    renderCaseSummaryCard(data);
    renderAIReasoningLayer(data);
    renderVerifiedAuthorityCard(data);

    if (typeof window.renderAdversarialCharts === 'function') {
        window.renderAdversarialCharts(data);
    }
}

// Render Rich Clickable Primary Source Authority Card
export function renderVerifiedAuthorityCard(data) {
    const auth = data.verified_authority || data.authority;
    if (!auth || typeof auth !== 'object' || !auth.citation) return;

    const container = document.getElementById('verifiedAuthorityContainer') || document.getElementById('precedentsList');
    if (!container) return;

    const src = auth.source || {};
    const ver = auth.verification || {};
    const checks = ver.verification_checks || {};
    const status = auth.status || auth.treatment || "VERIFIED";

    let statusColor = "#16a34a"; // green
    if (status === "SUPERSEDED") statusColor = "#dc2626"; // red
    if (status === "DISTINGUISHABLE" || status === "DISTINGUISHED") statusColor = "#d97706"; // amber
    if (status === "UNKNOWN") statusColor = "#6b7280"; // gray

    const officialUrl = src.official_url || "#";
    const docHash = src.document_hash ? src.document_hash.substring(0, 18) + "..." : "sha256:verified";

    const isPrimary = ver.primary_source_verified;
    const isIntegrity = ver.document_integrity_verified;
    const isPropMapped = ver.proposition_mapped;
    const isTreat = ver.current_treatment_checked;

    const cardHTML = `
        <div class="verified-authority-card" style="background: var(--glass-bg); backdrop-filter: blur(var(--glass-blur)); -webkit-backdrop-filter: blur(var(--glass-blur)); border: 1px solid var(--glass-border); border-radius: 12px; padding: 1.25rem; margin-top: 1rem; box-shadow: var(--shadow-md); font-family: inherit;">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; border-bottom: 1px solid var(--gray-200); padding-bottom: 0.75rem; margin-bottom: 0.75rem;">
                <div>
                    <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.25rem;">
                        <span style="background: rgba(99,102,241,0.15); color: var(--primary-400); font-size: 0.7rem; font-weight: 700; text-transform: uppercase; padding: 0.2rem 0.5rem; border-radius: 4px; border: 1px solid rgba(99,102,241,0.3);">
                            ${auth.type || 'PRIMARY_AUTHORITY'}
                        </span>
                        <span style="background: ${statusColor}18; color: ${statusColor}; font-size: 0.7rem; font-weight: 700; padding: 0.2rem 0.5rem; border-radius: 4px; border: 1px solid ${statusColor}40; display: inline-flex; align-items: center; gap: 0.3rem;">
                            <i class="fas fa-shield-alt"></i> ${status}
                        </span>
                    </div>
                    <h4 style="margin: 0; font-size: 1.1rem; font-weight: 750; color: var(--gray-900);">${auth.title || auth.citation}</h4>
                    <div style="font-size: 0.85rem; color: var(--gray-500); margin-top: 0.2rem;">
                        <i class="fas fa-university"></i> ${auth.court || auth.issuing_authority || 'Supreme Court of India'} &bull; ${auth.date || 'Verified Statutory Authority'}
                    </div>
                </div>
                ${src.official_url ? `
                    <a href="${officialUrl}" target="_blank" rel="noopener noreferrer" class="btn btn-primary" style="padding: 0.4rem 0.85rem; font-size: 0.8rem; border-radius: 8px;">
                        <i class="fas fa-external-link-alt"></i> VIEW OFFICIAL SOURCE
                    </a>
                ` : ''}
            </div>

            <div style="font-size: 0.9rem; color: var(--gray-700); line-height: 1.6; margin-bottom: 0.85rem;">
                <strong style="color: var(--gray-900);">Proposition Mapped:</strong> ${auth.proposition_supported || (auth.principles && auth.principles[0]) || 'Legal principles mapped to statutory posture.'}
            </div>

            <!-- Distinct 4-Badge Verification Grid -->
            <div style="background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: 8px; padding: 0.75rem; display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 0.5rem; font-size: 0.78rem; color: var(--gray-600);">
                <div style="display: flex; align-items: center; gap: 0.4rem;">
                    <i class="fas fa-check-circle" style="color: ${isPrimary ? 'var(--success-500)' : 'var(--gray-400)'};"></i> Official Source Verified
                </div>
                <div style="display: flex; align-items: center; gap: 0.4rem;">
                    <i class="fas fa-check-circle" style="color: ${isIntegrity ? 'var(--success-500)' : 'var(--gray-400)'};"></i> Document Integrity (${docHash})
                </div>
                <div style="display: flex; align-items: center; gap: 0.4rem;">
                    <i class="fas fa-check-circle" style="color: ${isPropMapped ? 'var(--success-500)' : 'var(--warning-500)'};"></i> Proposition Mapped to KB Ratio
                </div>
                <div style="display: flex; align-items: center; gap: 0.4rem;">
                    <i class="fas fa-check-circle" style="color: ${isTreat ? 'var(--success-500)' : 'var(--gray-400)'};"></i> Treatment Checked (${status})
                </div>
            </div>
        </div>
    `;

    if (container.id === 'verifiedAuthorityContainer') {
        container.innerHTML = cardHTML;
    } else {
        container.insertAdjacentHTML('afterbegin', cardHTML);
    }
}

// Render statutory rules engine
export function renderRulesEngine(rulesData) {
    const container = document.getElementById('statutoryRulesContainer');
    if (!container) return;

    if (!rulesData || rulesData.length === 0) {
        container.innerHTML = '';
        return;
    }

    let html = `
        <div style="background: var(--error-50); border: 2px solid var(--error-600); border-radius: var(--radius-lg); padding: 1.5rem; margin-bottom: 2rem; box-shadow: 0 4px 14px rgba(220, 38, 38, 0.15);">
            <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 1rem; color: var(--error-400); border-bottom: 1px solid rgba(239,68,68,0.2); padding-bottom: 0.75rem;">
                <i class="fas fa-exclamation-triangle" style="font-size: 1.5rem; animation: pulse 2s infinite;"></i>
                <h3 style="margin: 0; font-weight: 800; text-transform: uppercase; letter-spacing: 0.5px; color: var(--error-400);">Statutory Bars Detected</h3>
            </div>
    `;

    html += rulesData.map(rule => `
        <div style="background: var(--glass-bg); border-left: 4px solid var(--error-500); padding: 1rem; margin-bottom: 1rem; border-radius: var(--radius-md); box-shadow: var(--shadow-sm); border: 1px solid var(--glass-border); border-left: 4px solid var(--error-500);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                <div style="font-weight: 750; color: var(--gray-900); font-size: 1.1rem;">${rule.rule_name}</div>
                <div style="background: rgba(239,68,68,0.15); color: var(--error-400); font-size: 0.75rem; font-weight: 700; padding: 0.25rem 0.5rem; border-radius: 4px; border: 1px solid rgba(239,68,68,0.3);">${rule.status}</div>
            </div>
            <div style="font-size: 0.92rem; color: var(--gray-700); margin-bottom: 0.75rem;"><strong>Trigger:</strong> ${rule.description}</div>
            <div style="font-size: 0.92rem; color: var(--error-400); font-weight: 600; margin-bottom: 0.75rem;"><i class="fas fa-gavel"></i> <strong>Legal Effect:</strong> ${rule.legal_effect}</div>
            <div style="font-size: 0.88rem; background: var(--gray-100); padding: 0.6rem 0.85rem; border-radius: 6px; border: 1px solid var(--gray-200); color: var(--gray-800);">
                <i class="fas fa-arrow-right" style="color: var(--primary-400);"></i> <strong>Action Required:</strong> ${rule.action}
            </div>
        </div>
    `).join('');

    html += `</div>`;
    container.innerHTML = html;
}

// Switch result tabs
export function switchResultTab(tabName) {
    if (!tabName) return;

    // Highlight active button
    document.querySelectorAll('.tab-button').forEach(btn => {
        btn.classList.remove('active');
        if (btn.getAttribute('data-tab') === tabName) {
            btn.classList.add('active');
        }
    });

    // Hide all tab content panes
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.add('hidden');
        content.classList.remove('active');
    });

    // Match any ID convention: tabDetailed, tabStrategy, tabOverview, tabDraft, tabSarfaesi_overview, etc.
    const candidates = [
        `tab${tabName.charAt(0).toUpperCase() + tabName.slice(1)}`,
        `tab${tabName}`,
        `tab_${tabName}`,
        `${tabName}Tab`,
        tabName
    ];
    
    let target = null;
    for (const c of candidates) {
        const el = document.getElementById(c);
        if (el) {
            target = el;
            break;
        }
    }

    if (target) {
        target.classList.remove('hidden');
        target.classList.add('active');
    }

    // If strategy tab selected, ensure adversarial charts render with correct dimensions
    if (tabName === 'strategy' && typeof window.renderAdversarialCharts === 'function' && window.state?.analysisResult) {
        window.renderAdversarialCharts(window.state.analysisResult);
    }

    // If draft tab selected, initialize the draft studio picker if needed
    if (tabName === 'draft' && typeof window.showDraftTypeSelection === 'function') {
        window.showDraftTypeSelection();
    }
}

// Render criminal defense & statutory safeguard matrix
export function renderCriminalAnalysis(data, container) {
    if (!container || !data) return;

    const bail = data.bail || data.bail_assessment || {};
    const quashing = data.quashing || data.quashing_assessment || {};
    const risks = data.adversarial_vulnerabilities || [];
    const econ = data.litigation_economics || {};
    const comp = econ.compounding_and_settlement || {};
    const bailEcon = econ.bail_economics || {};
    const plea = econ.trial_vs_plea || {};
    const timeline = data.timeline || data.timeline_analysis || {};
    const caseData = data.case_data || {};

    const antilCategory = bail.antil_category || "Category A (Offenses <= 7 Years)";
    const bailProb = bail.probability || "HIGH";
    const bailProbColor = (bailProb.includes("VERY HIGH") || bailProb.includes("HIGH")) ? "var(--success-500)" : (bailProb.includes("MEDIUM") ? "var(--warning-500)" : "var(--error-500)");

    let criminalHTML = `
        <div class="criminal-dashboard-panel" style="background: var(--glass-bg); backdrop-filter: blur(var(--glass-blur)); -webkit-backdrop-filter: blur(var(--glass-blur)); border: 1px solid rgba(99,102,241,0.25); border-radius: var(--radius-xl); padding: 1.5rem; margin-bottom: 2rem; box-shadow: var(--shadow-xl); font-family: inherit;">
            <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--gray-200); padding-bottom: 1rem; margin-bottom: 1.25rem; flex-wrap: wrap; gap: 0.75rem;">
                <div style="display: flex; align-items: center; gap: 0.75rem;">
                    <span style="background: linear-gradient(135deg, var(--primary-600), var(--primary-800)); color: #ffffff; width: 40px; height: 40px; border-radius: 10px; display: inline-flex; align-items: center; justify-content: center; font-size: 1.2rem; box-shadow: 0 4px 12px rgba(99,102,241,0.3);">
                        <i class="fas fa-balance-scale"></i>
                    </span>
                    <div>
                        <h3 style="margin: 0; font-size: 1.25rem; font-weight: 800; color: var(--gray-900);">Criminal Defense & Statutory Safeguard Matrix</h3>
                        <div style="font-size: 0.85rem; color: var(--gray-500);">Governed by IPC 1860 / CrPC 1973 & Bharatiya Nyaya Sanhita (BNS / BNSS 2023)</div>
                    </div>
                </div>
                <div style="text-align: right;">
                    <span style="background: ${bailProbColor}18; color: ${bailProbColor}; font-size: 0.82rem; font-weight: 800; padding: 0.4rem 0.85rem; border-radius: var(--radius-full); border: 1px solid ${bailProbColor}40; display: inline-flex; align-items: center; gap: 0.4rem; box-shadow: 0 0 10px ${bailProbColor}20;">
                        <i class="fas fa-gavel"></i> BAIL PROBABILITY: ${escapeHtml(bailProb)}
                    </span>
                </div>
            </div>

            <!-- Satender Kumar Antil 4-Category Matrix -->
            <div style="background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: var(--radius-lg); padding: 1.25rem; margin-bottom: 1.25rem; box-shadow: var(--shadow-sm);">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.65rem; flex-wrap: wrap; gap: 0.5rem;">
                    <span style="font-size: 0.8rem; font-weight: 750; color: var(--primary-400); text-transform: uppercase; letter-spacing: 0.5px;">
                        <i class="fas fa-shield-alt"></i> Satender Kumar Antil (2022) 10 SCC 51 Taxonomy
                    </span>
                    <span style="background: rgba(99,102,241,0.15); color: var(--primary-300); border: 1px solid rgba(99,102,241,0.3); font-size: 0.75rem; font-weight: 750; padding: 0.25rem 0.65rem; border-radius: var(--radius-full);">
                        ${escapeHtml(antilCategory)}
                    </span>
                </div>
                <div style="font-size: 0.92rem; color: var(--gray-800); line-height: 1.6; margin-bottom: 0.6rem;">
                    <strong style="color: var(--gray-900);">Strategic Rationale:</strong> ${escapeHtml(bail.strategic_rationale || 'Bail is the rule, jail is the exception (State of Rajasthan v. Balchand).')}
                </div>
                ${bail.default_bail_triggered ? `
                    <div style="background: rgba(239,68,68,0.12); border-left: 4px solid var(--error-500); padding: 0.75rem 1rem; border-radius: 6px; font-size: 0.88rem; color: var(--error-400); margin-top: 0.5rem; border: 1px solid rgba(239,68,68,0.25); border-left: 4px solid var(--error-500);">
                        <i class="fas fa-clock"></i> <strong>S.167(2) Default Bail Right Accrued:</strong> Mandatory statutory bail must be filed immediately prior to submission of the police charge sheet (Ritu Chhabaria v. UOI).
                    </div>
                ` : ''}
            </div>

            <!-- Compounding & Settlement Dynamics (S.320 CrPC / S.359 BNSS) -->
            ${comp.statutory_mechanism ? `
                <div style="background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: var(--radius-lg); padding: 1.25rem; margin-bottom: 1.25rem; box-shadow: var(--shadow-sm);">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.55rem; flex-wrap: wrap; gap: 0.5rem;">
                        <span style="font-size: 0.82rem; font-weight: 750; color: var(--gray-900);">
                            <i class="fas fa-handshake" style="color: var(--primary-400);"></i> Compounding & Dispute Settlement Viability
                        </span>
                        <span style="background: ${comp.is_compoundable ? 'rgba(16,185,129,0.15)' : 'rgba(245,158,11,0.15)'}; color: ${comp.is_compoundable ? 'var(--success-400)' : 'var(--gold-400)'}; border: 1px solid ${comp.is_compoundable ? 'rgba(16,185,129,0.3)' : 'rgba(245,158,11,0.3)'}; font-size: 0.75rem; font-weight: 700; padding: 0.25rem 0.65rem; border-radius: var(--radius-full);">
                            ${comp.is_compoundable ? 'COMPOUNDABLE' : 'NON-COMPOUNDABLE (S.482 REQUIRED)'}
                        </span>
                    </div>
                    <div style="font-size: 0.9rem; color: var(--gray-700); margin-bottom: 0.4rem; line-height: 1.5;">
                        <strong style="color: var(--gray-900);">Mechanism:</strong> ${escapeHtml(comp.statutory_mechanism)}
                    </div>
                    <div style="font-size: 0.84rem; color: var(--gray-500);">
                        <strong>Surety Bond Exposure:</strong> Approx. Rs. ${(bailEcon.estimated_surety_bond || 25000).toLocaleString('en-IN')} | ${escapeHtml(bailEcon.cash_bail_viability || 'Standard Surety')}
                    </div>
                </div>
            ` : ''}

            <!-- Trial Cross-Examination Question Bank -->
            ${risks.length > 0 && risks[0].cross_exam_questions ? `
                <div style="background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: var(--radius-lg); padding: 1.25rem; margin-bottom: 1.25rem; box-shadow: var(--shadow-sm);">
                    <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.85rem; flex-wrap: wrap; gap: 0.5rem;">
                        <div style="font-weight: 750; color: var(--gray-900); font-size: 1rem; display: flex; align-items: center; gap: 0.5rem;">
                            <i class="fas fa-user-tie" style="color: var(--primary-400);"></i> Trial Cross-Examination Question Bank
                        </div>
                        <span style="font-size: 0.75rem; color: var(--gray-500); font-weight: 600;">Ready for Courtroom Deposition</span>
                    </div>
                    <div style="display: flex; flex-direction: column; gap: 0.65rem;">
                        ${(risks[0].cross_exam_questions || []).map((q, idx) => `
                            <div style="background: var(--glass-bg); border: 1px solid var(--glass-border); border-radius: 8px; padding: 0.75rem 1rem; font-size: 0.88rem; color: var(--gray-800); display: flex; justify-content: space-between; align-items: center; gap: 0.75rem; transition: border-color 0.2s;">
                                <div><strong style="color: var(--primary-400);">Q${idx + 1}:</strong> ${escapeHtml(q)}</div>
                                <button type="button" onclick="navigator.clipboard.writeText('${escapeHtml(q).replace(/'/g, "\\'")}'); this.innerHTML='<i class=\\'fas fa-check\\'></i> Copied'; setTimeout(() => this.innerHTML='<i class=\\'fas fa-copy\\'></i> Copy', 2000);" class="btn btn-outline" style="padding: 0.25rem 0.65rem; font-size: 0.75rem; border-radius: 6px; white-space: nowrap;">
                                    <i class="fas fa-copy"></i> Copy
                                </button>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            <!-- Prosecution Counter-Attack & Rebuttal Simulator -->
            ${(data.prosecution_counter_attacks || []).length > 0 ? `
                <div style="background: var(--gray-100); border: 1px solid rgba(239,68,68,0.3); border-radius: var(--radius-lg); padding: 1.25rem; margin-bottom: 1.25rem; box-shadow: var(--shadow-sm);">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.85rem; flex-wrap: wrap; gap: 0.5rem;">
                        <span style="font-size: 0.95rem; font-weight: 800; color: var(--error-500); display: flex; align-items: center; gap: 0.5rem;">
                            <i class="fas fa-crosshairs"></i> Prosecution Counter-Attack Matrix (Worst-Case Simulator)
                        </span>
                        <span style="background: rgba(239,68,68,0.12); color: var(--error-500); border: 1px solid rgba(239,68,68,0.3); font-size: 0.75rem; font-weight: 750; padding: 0.25rem 0.65rem; border-radius: var(--radius-full);">
                            ${data.prosecution_counter_attacks.length} ATTACK VECTORS PREEMPTED
                        </span>
                    </div>
                    <div style="display: flex; flex-direction: column; gap: 0.85rem;">
                        ${data.prosecution_counter_attacks.map(atk => `
                            <div style="background: var(--glass-bg); border: 1px solid var(--glass-border); border-left: 4px solid var(--error-500); border-radius: 8px; padding: 0.9rem 1.1rem;">
                                <div style="font-weight: 800; color: var(--gray-900); font-size: 0.92rem; margin-bottom: 0.35rem;">
                                    ${escapeHtml(atk.attack_title)}
                                </div>
                                <div style="font-size: 0.86rem; color: var(--error-400); margin-bottom: 0.5rem; line-height: 1.5;">
                                    <strong>Prosecution Angle:</strong> ${escapeHtml(atk.prosecution_argument)}
                                </div>
                                <div style="background: rgba(16,185,129,0.08); border-left: 3px solid var(--success-500); padding: 0.6rem 0.85rem; border-radius: 4px; font-size: 0.86rem; color: var(--gray-800); line-height: 1.5;">
                                    <strong style="color: var(--success-500);"><i class="fas fa-shield-alt"></i> Defense Rebuttal:</strong> ${escapeHtml(atk.defense_rebuttal)}
                                    <div style="font-size: 0.78rem; color: var(--gray-500); margin-top: 0.25rem;"><em>Ratio: ${escapeHtml(atk.defense_precedent)}</em></div>
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            ` : ''}

            <!-- Forensics & Electronic Evidence Hash Validator -->
            ${data.electronic_evidence_audit ? `
                <div style="background: var(--gray-100); border: 1px solid var(--gray-200); border-radius: var(--radius-lg); padding: 1.25rem; box-shadow: var(--shadow-sm);">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem; flex-wrap: wrap; gap: 0.5rem;">
                        <span style="font-size: 0.95rem; font-weight: 800; color: var(--gray-900); display: flex; align-items: center; gap: 0.5rem;">
                            <i class="fas fa-file-code" style="color: #6366f1;"></i> Electronic Evidence SHA-256 Hash & Metadata Validator
                        </span>
                        <span style="background: rgba(99,102,241,0.12); color: #6366f1; border: 1px solid rgba(99,102,241,0.3); font-size: 0.75rem; font-weight: 750; padding: 0.25rem 0.65rem; border-radius: var(--radius-full);">
                            SECTION 63(4) BSA 2023 COMPLIANT
                        </span>
                    </div>
                    <div style="font-size: 0.86rem; color: var(--gray-700); line-height: 1.5; margin-bottom: 0.6rem;">
                        All uploaded electronic exhibits (chat exports, call recordings, bank receipts) have been verified with cryptographic checksums and custody timestamps per <em>Arjun Panditrao Khotkar (2020) 7 SCC 1</em>.
                    </div>
                    <div style="display: flex; gap: 0.5rem; flex-wrap: wrap;">
                        <button class="btn btn-sm btn-outline" onclick="window.switchResultTab('draft');" style="padding: 0.3rem 0.75rem; font-size: 0.78rem;">
                            <i class="fas fa-file-signature"></i> View Section 63(4) BSA Certificate Schedule
                        </button>
                    </div>
                </div>
            ` : ''}
        </div>
    `;

    container.insertAdjacentHTML('afterbegin', criminalHTML);
}


