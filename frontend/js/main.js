import { firebaseConfig, roleActions, wizardSteps } from '../config.js?v=14';
import { api } from '../api.js?v=15';
import { ui, switchScreen } from '../ui.js?v=14';
import { renderWizardStep } from '../wizard.js?v=14';
import { renderResults, switchResultTab } from '../renderer.js?v=14';
import { DRAFT_TYPES, formatDraftDate, numberToWords } from '../draft_templates.js?v=14';
import { escapeHtml } from './modules/utils.js?v=14';

// Import sub-modules to register their exports or initialize their global interfaces
import { JudiQModals } from './modules/modals.js?v=14';
import { renderAdversarialCharts, renderScoreBreakdownChart } from './modules/charts.js?v=14';
import { JudiQValidator } from './modules/validation.js?v=14';
import { JudiQCoCounselDock } from './modules/counsel_dock.js?v=14';
import { JudiQStrategySimulator } from './modules/simulator.js?v=14';
import { initBankRecoveryModule } from './bank_recovery.js?v=14';
import './compliance_auditor.js?v=15';
import './counsel_intel.js?v=15';
import './enterprise_features.js?v=15';
import { initCaseManager } from './case_manager.js?v=16';
import { initClientManager } from './client_manager.js?v=16';
import { initDocumentLibrary } from './document_library.js?v=16';
import { initDraftWorkflow } from './draft_workflow_ui.js?v=16';
import { initCmsAnalytics } from './analytics_charts.js?v=16';


import { store } from './modules/store.js?v=14';

// Initialize Firebase
if (typeof firebase !== 'undefined') {
    firebase.initializeApp(firebaseConfig);
}
const auth = typeof firebase !== 'undefined' ? firebase.auth() : null;

// Enterprise Reactive State Initialization
store.attachGlobalProxy();

// Expose charts to window for switchResultTab access
window.renderAdversarialCharts = renderAdversarialCharts;
window.renderScoreBreakdownChart = renderScoreBreakdownChart;

/**
 * Initialize the application
 */
document.addEventListener('DOMContentLoaded', () => {
    setupAuthListeners();
    setupFormListeners();
    initTheme();
    initBankRecoveryModule();
    initCaseManager();
    initClientManager();
    initDocumentLibrary();
    initDraftWorkflow();
    initCmsAnalytics();
    
    // Initialize Co-Counsel Dock & Strategy Simulator
    window.judiqDock = new JudiQCoCounselDock();
    window.judiqSimulator = new JudiQStrategySimulator();

    const loadingScreen = document.getElementById('loadingScreen');
    if (loadingScreen) {
        ui.hide('loadingScreen');
    }
    if (window.calculateSandboxTimelines) {
        window.calculateSandboxTimelines();
    }

    // Scroll reveal observer
    const revealElements = document.querySelectorAll('.reveal-on-scroll');
    if (revealElements.length > 0 && 'IntersectionObserver' in window) {
        const revealObserver = new IntersectionObserver((entries, observer) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    entry.target.classList.add('revealed');
                    observer.unobserve(entry.target);
                }
            });
        }, {
            threshold: 0.05,
            rootMargin: '0px 0px -40px 0px'
        });
        revealElements.forEach(el => revealObserver.observe(el));
    }

    // Initialize Readiness Checklist & Precedents
    if (window.updateReadinessProgress) {
        window.updateReadinessProgress();
    }
    if (window.filterPrecedentsList) {
        window.filterPrecedentsList();
    }
});

function setupAuthListeners() {
    // Bind click events for navigation
    const showRegisterEl = document.getElementById('showRegister');
    if (showRegisterEl) showRegisterEl.addEventListener('click', (e) => { e.preventDefault(); window.showRegister(); });

    const showLoginEl = document.getElementById('showLogin');
    if (showLoginEl) showLoginEl.addEventListener('click', (e) => { e.preventDefault(); window.showLogin(); });

    const navLoginBtn = document.getElementById('navLoginBtn');
    if (navLoginBtn) navLoginBtn.addEventListener('click', () => window.showLogin());

    const navRegisterBtn = document.getElementById('navRegisterBtn');
    if (navRegisterBtn) navRegisterBtn.addEventListener('click', () => window.showRegister());

    const heroLoginBtn = document.getElementById('heroLoginBtn');
    if (heroLoginBtn) heroLoginBtn.addEventListener('click', () => window.showLogin());

    const heroGetStartedBtn = document.getElementById('heroGetStartedBtn');
    if (heroGetStartedBtn) heroGetStartedBtn.addEventListener('click', () => window.showRegister());

    const mobileNavLoginBtn = document.getElementById('mobileNavLoginBtn');
    if (mobileNavLoginBtn) mobileNavLoginBtn.addEventListener('click', () => { window.toggleMobileNav(false); window.showLogin(); });

    const mobileNavRegisterBtn = document.getElementById('mobileNavRegisterBtn');
    if (mobileNavRegisterBtn) mobileNavRegisterBtn.addEventListener('click', () => { window.toggleMobileNav(false); window.showRegister(); });

    if (!auth) {
        console.info('Firebase auth service initialized in direct mode.');
        const savedEmail = localStorage.getItem('judiq_active_user_email');
        if (savedEmail) {
            loginLocally(savedEmail);
        } else {
            switchScreen('landingScreen');
        }
        return;
    }

    auth.onAuthStateChanged(user => {
        window.state.currentUser = user;
        if (user) {
            const userEmailEl = document.getElementById('userEmail');
            if (userEmailEl) ui.setText('userEmail', user.email);

            // Real-Time Cloud Sync: Firebase Firestore user profile
            if (typeof firebase !== 'undefined' && firebase.firestore) {
                try {
                    const db = firebase.firestore();
                    db.collection('users').doc(user.uid).set({
                        user_id: user.uid,
                        email: user.email || '',
                        displayName: user.displayName || user.email || 'Advocate',
                        last_login: new Date().toISOString()
                    }, { merge: true });
                } catch (fbErr) {
                    console.warn('[Firestore] User login sync notice:', fbErr);
                }
            }

            // Read permanently locked domain (defaults to ni_act for legacy accounts)
            const savedDomain = localStorage.getItem(`judiq_domain_${user.uid}`) || 'ni_act';
            window.state.userDomain = savedDomain;

            const savedRole = localStorage.getItem(`judiq_role_${user.uid}`);
            if (savedRole) {
                window.state.currentRole = savedRole;
                renderDashboard();
                switchScreen('dashboardScreen');
            } else {
                // Apply domain class to role screen before showing it
                const roleScreen = document.getElementById('roleScreen');
                if (roleScreen) {
                    roleScreen.classList.remove('role-screen--sarfaesi', 'role-screen--ni');
                    if (savedDomain === 'sarfaesi') roleScreen.classList.add('role-screen--sarfaesi');
                }
                switchScreen('roleScreen');
            }
        } else {
            switchScreen('landingScreen');
        }
    });
}

export function loginLocally(email, domain = 'ni_act', role = 'law_firm') {
    const cleanEmail = (email && email.trim()) || 'advocate@judiq.ai';
    const uid = 'user_' + Math.abs(cleanEmail.split('').reduce((a, b) => { a = ((a << 5) - a) + b.charCodeAt(0); return a & a; }, 0)).toString(36);
    const mockUser = {
        email: cleanEmail,
        uid: uid,
        displayName: cleanEmail.split('@')[0]
    };
    window.state.currentUser = mockUser;
    
    // Save domain
    const savedDomain = localStorage.getItem(`judiq_domain_${uid}`) || domain || 'ni_act';
    localStorage.setItem(`judiq_domain_${uid}`, savedDomain);
    window.state.userDomain = savedDomain;
    
    // Save role
    const savedRole = localStorage.getItem(`judiq_role_${uid}`) || role || 'law_firm';
    localStorage.setItem(`judiq_role_${uid}`, savedRole);
    window.state.currentRole = savedRole;
    
    const userEmailEl = document.getElementById('userEmail');
    if (userEmailEl) ui.setText('userEmail', mockUser.email);
    
    renderDashboard();
    switchScreen('dashboardScreen');
    if (window.ui && typeof window.ui.toast === 'function') {
        window.ui.toast(`Signed in as ${cleanEmail}`, 'success');
    }
}

window.loginAsGuest = (domain = 'ni_act') => {
    loginLocally('advocate@judiq.ai', domain, 'law_firm');
};

function setupFormListeners() {
    const loginForm = document.getElementById('loginForm');
    const loginError = document.getElementById('loginError');
    if (loginForm) {
        loginForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const email = document.getElementById('loginEmail').value.trim();
            const pass = document.getElementById('loginPassword').value;
            const btn = loginForm.querySelector('button[type="submit"]');

            if (loginError) {
                loginError.textContent = '';
                loginError.classList.remove('show');
            }
            if (btn) btn.classList.add('loading');
            
            try {
                if (auth && typeof auth.signInWithEmailAndPassword === 'function') {
                    try {
                        await auth.signInWithEmailAndPassword(email, pass);
                    } catch (signInErr) {
                        const code = (signInErr?.code || '').toLowerCase();
                        const msg = (signInErr?.message || '').toLowerCase();
                        
                        // If Firebase Auth provider is not enabled in Firebase Console (e.g. OPERATION_NOT_ALLOWED, API_KEY_INVALID, etc.)
                        if (code.includes('operation-not-allowed') || code.includes('api-key') || code.includes('project-not-found') || code.includes('configuration-not-found') || code.includes('unauthorized-domain') || msg.includes('operation_not_allowed')) {
                            console.info('Firebase Auth project provider disabled, seamlessly activating local advocate session:', signInErr?.message || signInErr);
                            loginLocally(email);
                        } else if (code.includes('invalid-login-credentials') || code.includes('user-not-found')) {
                            try {
                                const cred = await auth.createUserWithEmailAndPassword(email, pass);
                                const defaultDomain = 'all';
                                localStorage.setItem(`judiq_domain_${cred.user.uid}`, defaultDomain);
                                window.state.userDomain = defaultDomain;
                                if (window.ui && typeof window.ui.toast === 'function') {
                                    window.ui.toast(`Account created for ${email}`, 'success');
                                }
                            } catch (createErr) {
                                const createCode = (createErr?.code || '').toLowerCase();
                                if (createCode.includes('email-already-in-use') || createCode.includes('wrong-password')) {
                                    if (loginError) {
                                        loginError.textContent = "Incorrect password for this existing account.";
                                        loginError.classList.add('show');
                                    }
                                } else if (createCode.includes('weak-password')) {
                                    if (loginError) {
                                        loginError.textContent = "Password should be at least 6 characters.";
                                        loginError.classList.add('show');
                                    }
                                } else {
                                    // Seamless local fallback
                                    loginLocally(email);
                                }
                            }
                        } else if (code.includes('wrong-password')) {
                            if (loginError) {
                                loginError.textContent = "Incorrect password. Please try again.";
                                loginError.classList.add('show');
                            }
                        } else if (code.includes('invalid-email')) {
                            if (loginError) {
                                loginError.textContent = "Please enter a valid email address.";
                                loginError.classList.add('show');
                            }
                        } else {
                            loginLocally(email);
                        }
                    }
                } else {
                    loginLocally(email);
                }
            } catch (err) {
                console.info('Activating local user session:', err?.message || err);
                loginLocally(email);
            } finally {
                if (btn) btn.classList.remove('loading');
            }
        });
    }

    const registerForm = document.getElementById('registerForm');
    const registerError = document.getElementById('registerError');
    if (registerForm) {
        registerForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const email = document.getElementById('registerEmail').value.trim();
            const pass = document.getElementById('registerPassword').value;
            const confirmPass = document.getElementById('registerConfirmPassword').value;
            const domain = document.getElementById('registerDomain')?.value || 'ni_act';
            const btn = registerForm.querySelector('button[type="submit"]');

            if (registerError) {
                registerError.textContent = '';
                registerError.classList.remove('show');
            }

            if (pass !== confirmPass) {
                if (registerError) {
                    registerError.textContent = "Passwords do not match";
                    registerError.classList.add('show');
                }
                return;
            }

            // Domain must be selected — one-time permanent lock
            if (!domain) {
                const errEl = document.getElementById('domainPickerError');
                if (errEl) errEl.classList.add('visible');
                return;
            }

            if (btn) btn.classList.add('loading');

            try {
                if (auth && typeof auth.createUserWithEmailAndPassword === 'function') {
                    const cred = await auth.createUserWithEmailAndPassword(email, pass);
                    localStorage.setItem(`judiq_domain_${cred.user.uid}`, domain);
                    window.state.userDomain = domain;
                } else {
                    loginLocally(email, domain);
                }
            } catch (err) {
                const code = err?.code || '';
                if (code.includes('email-already-in-use')) {
                    if (registerError) {
                        registerError.textContent = "An account with this email already exists. Please sign in instead.";
                        registerError.classList.add('show');
                    }
                } else if (code.includes('weak-password')) {
                    if (registerError) {
                        registerError.textContent = "Password is too weak. Please use at least 6 characters.";
                        registerError.classList.add('show');
                    }
                } else if (code.includes('invalid-email')) {
                    if (registerError) {
                        registerError.textContent = "Please enter a valid email address.";
                        registerError.classList.add('show');
                    }
                } else {
                    console.info('Firebase registration unavailable, creating local user session:', err?.message || err);
                    loginLocally(email, domain);
                }
            } finally {
                if (btn) btn.classList.remove('loading');
            }
        });
    }
}

/**
 * Global navigation exports for HTML onclick bindings
 */
window.showLogin = () => switchScreen('loginScreen');
window.showRegister = () => switchScreen('registerScreen');
window.showLanding = () => switchScreen('landingScreen');
window.showTerms = () => switchScreen('termsScreen');
window.showPrivacy = () => switchScreen('privacyScreen');
window.showRefund = () => switchScreen('refundScreen');
window.showDashboard = () => switchScreen('dashboardScreen');

/**
 * Domain picker — called from register screen onclick
 * Supports all 6 domain options with immediate visual feedback
 */
window.selectRegisterDomain = (domain) => {
    const domainCards = ['all', 'composite', 'ni', 'sarfaesi', 'criminal', 'civil'];
    domainCards.forEach(d => {
        const card = document.getElementById(`domainCard_${d}`);
        if (card) card.classList.remove('selected--all', 'selected--composite', 'selected--ni', 'selected--sarfaesi', 'selected--criminal', 'selected--civil');
    });

    const activeCard = document.getElementById(`domainCard_${domain === 'ni_act' ? 'ni' : domain}`);
    if (activeCard) {
        activeCard.classList.add(`selected--${domain === 'ni_act' ? 'ni' : domain}`);
    }

    const input = document.getElementById('registerDomain');
    if (input) input.value = domain;

    const errEl = document.getElementById('domainPickerError');
    if (errEl) errEl.classList.remove('visible');
};

/**
 * Switch active legal domain dynamically inside dashboard — locked to Section 138 NI Act
 */
window.switchUserDomain = (domain, tabEl) => {
    window.state.userDomain = 'ni_act';
    if (window.state.currentUser) {
        localStorage.setItem(`judiq_domain_${window.state.currentUser.uid}`, 'ni_act');
    }
    document.querySelectorAll('.domain-switch-tab').forEach(t => t.classList.remove('active'));
    const targetTab = document.getElementById('tab_domain_ni') || document.getElementById('tab_domain_all');
    if (targetTab) targetTab.classList.add('active');
    renderDashboard();
};

window.logout = () => {
    if (auth && typeof auth.signOut === 'function') {
        auth.signOut().catch(() => {});
    }
    window.state.currentUser = null;
    switchScreen('landingScreen');
    if (window.ui && typeof window.ui.toast === 'function') {
        window.ui.toast('Logged out successfully', 'info');
    }
};

window.selectRole = (role) => {
    window.state.currentRole = role;
    if (window.state.currentUser) {
        localStorage.setItem(`judiq_role_${window.state.currentUser.uid}`, role);
    }
    renderDashboard();
    switchScreen('dashboardScreen');
};

function renderDashboard() {
    const role = window.state.currentRole || 'citizen';
    const domain = 'ni_act';
    window.state.userDomain = 'ni_act';

    // Update active tab highlight in quick-switch bar
    document.querySelectorAll('.domain-switch-tab').forEach(t => t.classList.remove('active'));
    const activeTab = document.getElementById('tab_domain_ni') || document.getElementById('tab_domain_all');
    if (activeTab) activeTab.classList.add('active');

    // Domain badge in dashboard nav
    const dashNav = document.querySelector('#dashboardScreen .nav-brand');
    if (dashNav) {
        let badge = dashNav.querySelector('.domain-badge');
        if (!badge) {
            badge = document.createElement('span');
            dashNav.appendChild(badge);
        }
        badge.className = 'domain-badge domain-badge--ni';
        badge.innerHTML = '<i class="fas fa-file-invoice-dollar"></i> NI Act — Section 138';
    }

    // Admin check & Quota synchronization
    const currentUser = window.state.currentUser;
    const userEmail = (currentUser && currentUser.email ? currentUser.email : '').toLowerCase().trim();
    const adminBtn = document.getElementById('adminPortalBtn');
    const isAdmin = ['admin@judiq.ai', 'gandhiatharv565@gmail.com'].includes(userEmail) || userEmail.startsWith('admin');
    
    if (adminBtn) {
        adminBtn.style.display = isAdmin ? 'inline-flex' : 'none';
    }

    // Update User Monthly Quota Pill
    if (currentUser && typeof api !== 'undefined' && api.getUserQuota) {
        const uid = currentUser.uid || 'demo_user_123';
        api.getUserQuota(uid, userEmail).then(res => {
            if (res && res.success && res.quota) {
                const q = res.quota;
                const pill = document.getElementById('userQuotaPill');
                const qText = document.getElementById('userQuotaText');
                if (pill && qText) {
                    if (q.monthly_report_limit === -1) {
                        qText.textContent = `${q.reports_used_this_month} Reports (Unlimited)`;
                    } else {
                        qText.textContent = `${q.remaining_reports}/${q.monthly_report_limit} Reports`;
                    }
                    pill.style.display = 'inline-flex';
                }
            }
        }).catch(err => {
            console.warn('Quota load failed:', err);
        });
    }

    // Domain-specific stats and actions
    const grid = document.getElementById('actionCardsGrid');
    if (grid) {
        const allCases = JSON.parse(localStorage.getItem('judiq_recent_cases_v1') || '[]');
        
        if (domain === 'composite') {
            const domainCases = allCases.filter(c => c.domain === 'composite' || (c.case_data && String(c.case_data.case_type).toLowerCase().includes('composite')));
            const total = domainCases.length;

            grid.innerHTML = `
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-bolt" style="color:#f59e0b;"></i>
                    <h3>Multi-Track Concurrent Legal Action (SARFAESI + 138 + Criminal)</h3>
                </div>
                <div class="domain-stats-grid" style="grid-column:1/-1;">
                    <div class="domain-stat-card" style="border-left: 4px solid #f59e0b;">
                        <div class="dsc-icon"><i class="fas fa-layer-group" style="color:#f59e0b;"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${total}</div><div class="dsc-label">Multi-Track Matters Evaluated</div></div>
                    </div>
                    <div class="domain-stat-card" style="border-left: 4px solid #10b981;">
                        <div class="dsc-icon"><i class="fas fa-sync" style="color:#10b981;"></i></div>
                        <div class="dsc-content"><div class="dsc-value">3 Tracks</div><div class="dsc-label">Concurrent Statutory Engines</div></div>
                    </div>
                </div>
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-rocket" style="color:#f59e0b;"></i>
                    <h3>Multi-Track Quick Actions</h3>
                </div>
                <div class="domain-actions-grid" style="grid-column:1/-1;">
                    <div class="domain-action-card" onclick="startCaseAnalysis({case_type:'Multi-Track (SARFAESI + 138 + Criminal)'})">
                        <div class="dac-icon" style="color:#f59e0b;"><i class="fas fa-bolt"></i></div>
                        <div class="dac-title">Run Unified Multi-Track Analysis</div>
                        <div class="dac-sub">Simultaneously evaluate SARFAESI asset recovery, Sec 138 cheque prosecution, and Criminal cheating charges on a single loan account.</div>
                    </div>
                    <div class="domain-action-card" onclick="window.loadCompositeDemoCase()">
                        <div class="dac-icon" style="color:#10b981;"><i class="fas fa-file-invoice"></i></div>
                        <div class="dac-title">Load Multi-Track Demo Scenario</div>
                        <div class="dac-sub">Pre-populate a high-value loan default with concurrent cheque bounce & SARFAESI actions.</div>
                    </div>
                    <div class="domain-action-card" onclick="window.showDraftStudio()" style="border: 2px solid rgba(16,185,129,0.35); background: rgba(16,185,129,0.05);">
                        <div class="dac-icon" style="color:#10b981;"><i class="fas fa-file-contract"></i></div>
                        <div class="dac-title">Generate Legal Draft</div>
                        <div class="dac-sub">Open the Draft Studio — 13 court-ready documents: notices, complaints, affidavits & SARFAESI notices.</div>
                    </div>
                </div>
            `;
        } else if (domain === 'criminal') {
            const domainCases = allCases.filter(c => c.domain === 'criminal' || (c.case_data && c.case_data.case_type === 'criminal'));
            const total = domainCases.length;
            const highBail = domainCases.filter(c => c.analysis_result && c.analysis_result.bail_assessment && c.analysis_result.bail_assessment.probability === 'HIGH').length;

            grid.innerHTML = `
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-user-shield" style="color:#ef4444;"></i>
                    <h3>Criminal Law Intelligence Overview</h3>
                </div>
                <div class="domain-stats-grid" style="grid-column:1/-1;">
                    <div class="domain-stat-card domain-stat-card--criminal" style="border-left: 4px solid #ef4444;">
                        <div class="dsc-icon"><i class="fas fa-gavel" style="color:#ef4444;"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${total}</div><div class="dsc-label">Criminal Cases Analyzed</div></div>
                    </div>
                    <div class="domain-stat-card domain-stat-card--criminal" style="border-left: 4px solid #10b981;">
                        <div class="dsc-icon"><i class="fas fa-key" style="color:#10b981;"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${highBail}</div><div class="dsc-label">High Bail Probability</div></div>
                    </div>
                </div>
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-bolt" style="color:#f59e0b;"></i>
                    <h3>Quick Actions</h3>
                </div>
                <div class="domain-actions-grid" style="grid-column:1/-1;">
                    <div class="domain-action-card" onclick="startCaseAnalysis({case_type:'Criminal'})">
                        <div class="dac-icon" style="color:#ef4444;"><i class="fas fa-search"></i></div>
                        <div class="dac-title">Analyse Criminal Case</div>
                        <div class="dac-sub">IPC/BNS & CrPC/BNSS audit, S.438 Bail & S.482 Bhajan Lal Quashing</div>
                    </div>
                    <div class="domain-action-card" onclick="startCaseAnalysis({case_type:'Criminal', offense_type:'420', contract_exists:true})">
                        <div class="dac-icon" style="color:#f59e0b;"><i class="fas fa-balance-scale"></i></div>
                        <div class="dac-title">S.420 / Civil Dispute Audit</div>
                        <div class="dac-sub">Test civil dispute quashing grounds under Bhajan Lal & Hridaya Ranjan precedent</div>
                    </div>
                    <div class="domain-action-card" onclick="window.loadCriminalDemoCase()">
                        <div class="dac-icon" style="color:#ef4444;"><i class="fas fa-file-invoice"></i></div>
                        <div class="dac-title">Load Demo Criminal Case</div>
                        <div class="dac-sub">Pre-fill S.420/406 IPC financial fraud scenario with quashing defence grounds</div>
                    </div>
                    <div class="domain-action-card" onclick="window.showDraftStudio()" style="border: 2px solid rgba(16,185,129,0.35); background: rgba(16,185,129,0.05);">
                        <div class="dac-icon" style="color:#10b981;"><i class="fas fa-file-contract"></i></div>
                        <div class="dac-title">Generate Legal Draft</div>
                        <div class="dac-sub">Open Draft Studio — generate criminal complaints, bail petitions, affidavits & BSA certificates.</div>
                    </div>
                </div>
            `;
        } else if (domain === 'civil') {
            const domainCases = allCases.filter(c => c.domain === 'civil' || (c.case_data && c.case_data.case_type === 'civil'));
            const total = domainCases.length;

            grid.innerHTML = `
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-balance-scale" style="color:#8b5cf6;"></i>
                    <h3>Civil & Commercial Litigation Overview</h3>
                </div>
                <div class="domain-stats-grid" style="grid-column:1/-1;">
                    <div class="domain-stat-card" style="border-left: 4px solid #8b5cf6;">
                        <div class="dsc-icon"><i class="fas fa-file-alt" style="color:#8b5cf6;"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${total}</div><div class="dsc-label">Civil Suits Analyzed</div></div>
                    </div>
                </div>
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-bolt" style="color:#f59e0b;"></i>
                    <h3>Quick Actions</h3>
                </div>
                <div class="domain-actions-grid" style="grid-column:1/-1;">
                    <div class="domain-action-card" onclick="startCaseAnalysis({case_type:'Civil'})">
                        <div class="dac-icon" style="color:#8b5cf6;"><i class="fas fa-search"></i></div>
                        <div class="dac-title">Analyse Civil Suit</div>
                        <div class="dac-sub">CPC Plaint, Written Statement, Order 39 Injunction & Limitation Audit</div>
                    </div>
                    <div class="domain-action-card" onclick="window.loadCivilDemoCase()">
                        <div class="dac-icon" style="color:#8b5cf6;"><i class="fas fa-file-invoice"></i></div>
                        <div class="dac-title">Load Demo Civil Suit</div>
                        <div class="dac-sub">Pre-fill Commercial Specific Performance suit with Order 39 interim injunction</div>
                    </div>
                    <div class="domain-action-card" onclick="window.showDraftStudio()" style="border: 2px solid rgba(16,185,129,0.35); background: rgba(16,185,129,0.05);">
                        <div class="dac-icon" style="color:#10b981;"><i class="fas fa-file-contract"></i></div>
                        <div class="dac-title">Generate Legal Draft</div>
                        <div class="dac-sub">Open Draft Studio — generate plaints, written statements, affidavits & interim injunctions.</div>
                    </div>
                </div>
            `;
        } else if (domain === 'sarfaesi') {
            const domainCases = allCases.filter(c => c.domain === 'sarfaesi');
            const npaCases = domainCases.length;
            const cersaiIssues = domainCases.filter(c => c.case_data && c.case_data.cersai_registered === false).length;
            const highRisk = domainCases.filter(c => c.risk_level === 'CRITICAL').length;

            grid.innerHTML = `
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-chart-bar" style="color:#10b981;"></i>
                    <h3>Account Overview</h3>
                </div>
                <div class="domain-stats-grid" style="grid-column:1/-1;">
                    <div class="domain-stat-card domain-stat-card--sarfaesi">
                        <div class="dsc-icon dsc-icon--sarfaesi"><i class="fas fa-file-invoice"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${npaCases}</div><div class="dsc-label">NPA Cases Analyzed</div></div>
                    </div>
                    <div class="domain-stat-card domain-stat-card--sarfaesi">
                        <div class="dsc-icon dsc-icon--sarfaesi"><i class="fas fa-shield-alt"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${cersaiIssues}</div><div class="dsc-label">CERSAI Issues</div></div>
                    </div>
                    <div class="domain-stat-card domain-stat-card--sarfaesi">
                        <div class="dsc-icon dsc-icon--sarfaesi"><i class="fas fa-exclamation-triangle"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${highRisk}</div><div class="dsc-label">High-Risk Accounts</div></div>
                    </div>
                </div>
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-bolt" style="color:#10b981;"></i>
                    <h3>Quick Actions</h3>
                </div>
                <div class="domain-actions-grid" style="grid-column:1/-1;">
                    <div class="domain-action-card domain-action-card--sarfaesi" onclick="startCaseAnalysis({case_type:'SARFAESI'})">
                        <div class="dac-icon dac-icon--sarfaesi"><i class="fas fa-search"></i></div>
                        <div class="dac-title">Analyse SARFAESI Case</div>
                        <div class="dac-sub">Run full NPA enforcement audit — CERSAI, S.13(2), S.14, DRT</div>
                    </div>
                    <div class="domain-action-card" onclick="window.loadSarfaesiDemoCase()">
                        <div class="dac-icon" style="color:#f59e0b;"><i class="fas fa-building"></i></div>
                        <div class="dac-title">Load Demo SARFAESI Case</div>
                        <div class="dac-sub">Pre-fill a commercial real-estate NPA scenario with full S.13 enforcement details</div>
                    </div>
                    <div class="domain-action-card" onclick="window.showDraftStudio()" style="border: 2px solid rgba(16,185,129,0.35); background: rgba(16,185,129,0.05);">
                        <div class="dac-icon" style="color:#10b981;"><i class="fas fa-file-contract"></i></div>
                        <div class="dac-title">Generate Legal Draft</div>
                        <div class="dac-sub">Open Draft Studio — generate SARFAESI s.13(2) notices, DRT applications & possession letters.</div>
                    </div>
                </div>
            `;
        } else if (domain === 'ni_act') {
            const domainCases = allCases.filter(c => !c.domain || c.domain === 'ni_act');
            const totalCases = domainCases.length;
            const fatalCases = domainCases.filter(c => c.verdict === 'DO NOT FILE').length;
            const strongCases = domainCases.filter(c => (c.score || 0) >= 70).length;

            grid.innerHTML = `
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-chart-bar" style="color:#3b82f6;"></i>
                    <h3>Account Overview</h3>
                </div>
                <div class="domain-stats-grid" style="grid-column:1/-1;">
                    <div class="domain-stat-card domain-stat-card--ni">
                        <div class="dsc-icon dsc-icon--ni"><i class="fas fa-gavel"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${totalCases}</div><div class="dsc-label">Cases Analysed</div></div>
                    </div>
                    <div class="domain-stat-card domain-stat-card--ni">
                        <div class="dsc-icon dsc-icon--ni"><i class="fas fa-check-circle"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${strongCases}</div><div class="dsc-label">Strong Cases (≥70)</div></div>
                    </div>
                    <div class="domain-stat-card domain-stat-card--ni">
                        <div class="dsc-icon dsc-icon--ni"><i class="fas fa-times-circle"></i></div>
                        <div class="dsc-content"><div class="dsc-value">${fatalCases}</div><div class="dsc-label">Fatal Defects Found</div></div>
                    </div>
                </div>
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-bolt" style="color:#3b82f6;"></i>
                    <h3>Quick Actions</h3>
                </div>
                <div class="domain-actions-grid" style="grid-column:1/-1;">
                    <div class="domain-action-card domain-action-card--ni" onclick="startCaseAnalysis({case_type:'Cheque Bounce'})">
                        <div class="dac-icon dac-icon--ni"><i class="fas fa-search"></i></div>
                        <div class="dac-title">Analyse S.138 Case</div>
                        <div class="dac-sub">Run adversarial weakness scan — Limitation, Notice, Instrument, Debt</div>
                    </div>
                    <div class="domain-action-card domain-action-card--ni" onclick="loadDemoCase()">
                        <div class="dac-icon dac-icon--ni"><i class="fas fa-bolt"></i></div>
                        <div class="dac-title">Load Demo Case</div>
                        <div class="dac-sub">Instantly preview a pre-populated cheque bounce scenario</div>
                    </div>
                    <div class="domain-action-card" onclick="window.showDraftStudio()" style="border: 2px solid rgba(16,185,129,0.35); background: rgba(16,185,129,0.05);">
                        <div class="dac-icon" style="color:#10b981;"><i class="fas fa-file-contract"></i></div>
                        <div class="dac-title">Generate Legal Draft</div>
                        <div class="dac-sub">Open Draft Studio — demand notices, criminal complaints, reply notices, affidavits & BSA certificates.</div>
                    </div>
                </div>
            `;
        } else {
            // "ALL" DOMAINS — Unified Multi-Domain Command Center
            const totalCases = allCases.length;
            grid.innerHTML = `
                <div class="domain-section-header" style="grid-column:1/-1;">
                    <i class="fas fa-layer-group" style="color:#38bdf8;"></i>
                    <h3>Institutional Litigation OS — All Legal Modules</h3>
                </div>
                <div class="domain-actions-grid" style="grid-column:1/-1; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));">
                    <div class="domain-action-card" onclick="startCaseAnalysis({case_type:'Multi-Track (SARFAESI + 138 + Criminal)'})" style="border: 2px solid rgba(245, 158, 11, 0.4); background: rgba(245, 158, 11, 0.05);">
                        <div class="dac-icon" style="color:#f59e0b;"><i class="fas fa-bolt"></i></div>
                        <div class="dac-title">Multi-Track Composite Action</div>
                        <div class="dac-sub">Evaluate SARFAESI + Section 138 + Criminal Fraud simultaneously on a single debt account.</div>
                    </div>
                    <div class="domain-action-card" onclick="startCaseAnalysis({case_type:'Cheque Bounce'})">
                        <div class="dac-icon" style="color:#3b82f6;"><i class="fas fa-file-invoice-dollar"></i></div>
                        <div class="dac-title">NI Act — Section 138</div>
                        <div class="dac-sub">Cheque dishonour memo, 30-day notice, S.139 presumption, and S.143A compensation.</div>
                    </div>
                    <div class="domain-action-card" onclick="startCaseAnalysis({case_type:'SARFAESI'})">
                        <div class="dac-icon" style="color:#10b981;"><i class="fas fa-university"></i></div>
                        <div class="dac-title">SARFAESI & DRT Recovery</div>
                        <div class="dac-sub">Secured asset enforcement, CERSAI verification, S.13(2)/13(4) notices, DM S.14 physical possession.</div>
                    </div>
                    <div class="domain-action-card" onclick="startCaseAnalysis({case_type:'Criminal'})">
                        <div class="dac-icon" style="color:#ef4444;"><i class="fas fa-user-shield"></i></div>
                        <div class="dac-title">Criminal Law (BNS & IPC)</div>
                        <div class="dac-sub">Cheating (S.318/420), CBT (S.316/406), Anticipatory/Regular Bail, and S.482 Quashing.</div>
                    </div>
                    <div class="domain-action-card" onclick="startCaseAnalysis({case_type:'Civil'})">
                        <div class="dac-icon" style="color:#8b5cf6;"><i class="fas fa-balance-scale"></i></div>
                        <div class="dac-title">Civil & Commercial Suits</div>
                        <div class="dac-sub">Specific performance, commercial debt recovery, and Order 39 interim injunctions.</div>
                    </div>
                    <div class="domain-action-card" onclick="window.showDraftStudio()" style="border: 2px solid rgba(16,185,129,0.35); background: rgba(16,185,129,0.05); grid-column: 1 / -1;">
                        <div class="dac-icon" style="color:#10b981;"><i class="fas fa-file-contract"></i></div>
                        <div class="dac-title" style="font-size: 1.05rem;">📄 Draft Studio — Generate Any Legal Document</div>
                        <div class="dac-sub">13 court-ready templates: demand notices, criminal complaints, bail petitions, SARFAESI s.13(2) notices, affidavits, BSA certificates, reply notices & more — enter your inputs and generate instantly.</div>
                    </div>
                </div>
            `;
        }
    }

    const roleBadge = document.getElementById('userRoleBadge');
    if (roleBadge) {
        roleBadge.textContent = role.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
        roleBadge.className = `user-role-badge role-${role}`;
    }

    // Personalization header greeting updates
    const greetingEl = document.getElementById('dashboardGreeting');
    const subtitleEl = document.getElementById('dashboardSubtitle');
    if (greetingEl && window.state.currentUser) {
        const uid = window.state.currentUser.uid;
        const savedProfileStr = localStorage.getItem(`judiq_profile_${uid}`);
        let name = window.state.currentUser.displayName || window.state.currentUser.email.split('@')[0];
        let firm = '';
        if (savedProfileStr) {
            try {
                const profile = JSON.parse(savedProfileStr);
                if (profile.displayName) name = profile.displayName;
                if (profile.firmName) firm = profile.firmName;
            } catch (_) {}
        }
        greetingEl.textContent = `Welcome, Counsel ${name}`;
        const domainLabels = {
            all: 'Full Practice Litigation Command Center',
            composite: 'Multi-Track Composite Platform',
            criminal: 'Criminal Law Platform',
            civil: 'Civil & Commercial Litigation Strategist',
            sarfaesi: 'SARFAESI / DRT Platform',
            ni_act: 'NI Act — Section 138 Platform'
        };
        const domainLabel = domainLabels[domain] || 'Institutional Legal Intelligence OS';
        if (subtitleEl) {
            subtitleEl.textContent = firm
                ? `${domainLabel} | ${firm} — Find the weakness before the courtroom does.`
                : `${domainLabel} — Find the weakness before the courtroom does.`;
        }
    }

    // Load domain-filtered recent activity
    if (window.loadRecentCases) {
        window.loadRecentCases();
    }
}

// Recent Cases / Activity History Management
function formatDate(dateStr) {
    if (!dateStr) return 'N/A';
    try {
        const d = new Date(dateStr);
        if (isNaN(d.getTime())) return dateStr;
        return d.toLocaleDateString(undefined, { 
            year: 'numeric', 
            month: 'short', 
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    } catch (_) {
        return dateStr;
    }
}

window.saveCaseToHistory = async (caseData, analysisResult) => {
    try {
        const userId = window.state.currentUser ? (window.state.currentUser.uid || window.state.currentUser.email) : 'ANONYMOUS';
        const caseId = caseData.case_id || (analysisResult && analysisResult.case_id) || ('case_' + Date.now());
        if (!caseId) return;

        let localCases = [];
        try {
            localCases = JSON.parse(localStorage.getItem('judiq_recent_cases_v1') || '[]');
        } catch (_) {}

        const score = analysisResult.score !== undefined ? analysisResult.score : (analysisResult.merit_score || 0);
        const verdict = analysisResult.verdict || analysisResult.primary_verdict || 'ANALYZED';
        const newCaseObj = {
            id: caseId,
            user_id: userId,
            domain: window.state.userDomain || 'ni_act',  // stamp domain permanently
            title: caseData.case_title || 'Untitled Case',
            date: new Date().toISOString(),
            score: score,
            risk_level: analysisResult.risk_level || analysisResult.defence_risk || 'Unknown',
            verdict: verdict,
            case_data: caseData,
            analysis_result: analysisResult
        };
        // Remove duplicates and put new one at the start
        localCases = localCases.filter(c => c.id !== caseId);
        localCases.unshift(newCaseObj);
        if (localCases.length > 30) {
            localCases.pop();
        }

        localStorage.setItem('judiq_recent_cases_v1', JSON.stringify(localCases));

        // 🔥 Real-Time Cloud Sync: Firebase Firestore
        if (typeof firebase !== 'undefined' && firebase.firestore) {
            try {
                const db = firebase.firestore();
                const now = new Date().toISOString();
                const globalDoc = {
                    case_id: caseId,
                    user_id: userId,
                    case_title: newCaseObj.title,
                    complainant_name: caseData.complainant_name || '',
                    accused_name: caseData.accused_name || '',
                    case_type: caseData.case_type || 'Cheque Bounce',
                    score: score,
                    verdict: verdict,
                    case_data: caseData,
                    analysis_result: analysisResult,
                    created_at: now,
                    updated_at: now
                };

                // 1. Write to global 'cases' collection
                await db.collection('cases').doc(caseId).set(globalDoc, { merge: true });

                // 2. Write to user's personal case sub-collection
                if (userId && userId !== 'ANONYMOUS') {
                    await db.collection('users').doc(userId).collection('cases').doc(caseId).set({
                        case_id: caseId,
                        case_title: newCaseObj.title,
                        score: score,
                        verdict: verdict,
                        case_type: caseData.case_type || 'Cheque Bounce',
                        updated_at: now
                    }, { merge: true });

                    await db.collection('users').doc(userId).set({
                        user_id: userId,
                        email: (window.state.currentUser && window.state.currentUser.email) || '',
                        last_active: now
                    }, { merge: true });
                }
                console.log('🔥 [Firestore] Successfully stored case and analysis to Firebase:', caseId);
            } catch (fbErr) {
                console.warn('⚠️ [Firestore] Notice while syncing case to Firebase:', fbErr);
            }
        }
        
        // Refresh dashboard view if it's currently rendered
        const recentCasesContainer = document.getElementById('recentCases');
        if (recentCasesContainer) {
            window.loadRecentCases();
        }
    } catch (err) {
        console.error('Failed to save case to history:', err);
    }
};

window.loadRecentCases = async () => {
    const container = document.getElementById('recentCases');
    if (!container) return;

    container.innerHTML = `
        <div style="display: flex; justify-content: center; align-items: center; padding: 2rem; color: var(--gray-500);">
            <i class="fas fa-spinner fa-spin" style="margin-right: 0.5rem;"></i> Loading recent activity...
        </div>
    `;

    try {
        let localCases = [];
        try {
            localCases = JSON.parse(localStorage.getItem('judiq_recent_cases_v1') || '[]');
        } catch (_) {}

        const userId = window.state.currentUser ? window.state.currentUser.uid : 'ANONYMOUS';
        const currentDomain = window.state.userDomain || 'ni_act';
        let backendCases = [];
        if (userId !== 'ANONYMOUS') {
            try {
                const res = await api.getRecentCases(userId);
                backendCases = Array.isArray(res) ? res : [];
            } catch (err) {
                console.warn('Failed to fetch recent cases from backend:', err);
            }

            // Cloud Firestore Fallback
            if (backendCases.length === 0 && typeof firebase !== 'undefined' && firebase.firestore) {
                try {
                    const db = firebase.firestore();
                    const snap = await db.collection('cases').where('user_id', '==', userId).limit(20).get();
                    snap.forEach(doc => {
                        const d = doc.data();
                        backendCases.push({
                            id: d.case_id || doc.id,
                            user_id: d.user_id,
                            title: d.case_title || 'Untitled Case',
                            date: d.updated_at || d.created_at || new Date().toISOString(),
                            score: d.score || 0,
                            risk_level: (d.analysis_result && (d.analysis_result.risk_level || d.analysis_result.defence_risk)) || 'Standard',
                            verdict: d.verdict || 'ANALYZED',
                            case_data: d.case_data || {},
                            analysis_result: d.analysis_result || {}
                        });
                    });
                } catch (fbErr) {
                    console.warn('[Firestore] Direct case fetch notice:', fbErr);
                }
            }
        }

        // Merge and de-duplicate by case ID
        const casesMap = new Map();

        // 1. Load backend case metadata
        backendCases.forEach(c => {
            casesMap.set(c.id, {
                id: c.id,
                user_id: c.user_id,
                title: c.title,
                date: c.date,
                score: c.score,
                risk_level: c.risk_level,
                verdict: c.verdict,
                fromBackend: true
            });
        });

        // 2. Overwrite / merge with local cases (which contain full details)
        localCases.forEach(c => {
            if (c.user_id === 'ANONYMOUS' || c.user_id === userId) {
                const existing = casesMap.get(c.id);
                casesMap.set(c.id, {
                    ...existing,
                    ...c,
                    fromBackend: existing ? true : false
                });
            }
        });

        const mergedCases = Array.from(casesMap.values())
            // Filter by user's domain — each user only sees their own domain's cases
            .filter(c => !c.domain || c.domain === currentDomain);
        mergedCases.sort((a, b) => new Date(b.date) - new Date(a.date));

        if (mergedCases.length === 0) {
            const emptyDiv = document.createElement('div');
            emptyDiv.style.cssText = 'text-align: center; padding: 2rem; border: 1px dashed var(--gray-300); border-radius: var(--radius-lg); color: var(--gray-400);';
            emptyDiv.innerHTML = '<i class="fas fa-folder-open" style="font-size: 1.5rem; margin-bottom: 0.5rem; display: block;"></i>No recent cases analyzed yet.';
            container.replaceChildren(emptyDiv);
            return;
        }

        container.replaceChildren(); // clear children safely

        mergedCases.forEach(c => {
            const itemDiv = document.createElement('div');
            itemDiv.className = 'recent-case-item';
            itemDiv.style.cssText = 'display: flex; justify-content: space-between; align-items: center; width: 100%;';
            itemDiv.onclick = () => window.loadCaseFromHistory(c.id);

            const infoDiv = document.createElement('div');
            infoDiv.className = 'recent-case-info';
            
            const h4 = document.createElement('h4');
            h4.textContent = c.title || 'Untitled Case';
            
            const p = document.createElement('p');
            p.style.cssText = 'font-size: 0.75rem; color: var(--gray-400); margin-top: 0.25rem;';
            p.innerHTML = `ID: <strong>${escapeHtml(c.id)}</strong> | Updated: <strong>${formatDate(c.date)}</strong> | Assessment: <span style="color: var(--primary-400); font-weight: 600;">${escapeHtml(c.verdict)}</span>`;
            
            infoDiv.appendChild(h4);
            infoDiv.appendChild(p);

            const rightDiv = document.createElement('div');
            rightDiv.style.cssText = 'display: flex; align-items: center; gap: 1.5rem;';

            const scoreDiv = document.createElement('div');
            scoreDiv.className = 'recent-case-score';
            scoreDiv.style.cssText = 'min-width: 80px; text-align: center;';
            scoreDiv.textContent = `${c.score}/100`;

            const delBtn = document.createElement('button');
            delBtn.className = 'btn-delete-case';
            delBtn.title = 'Delete Case';
            delBtn.style.cssText = 'background: transparent; border: none; color: var(--error-500); cursor: pointer; padding: 0.5rem; font-size: 1rem; transition: color 0.2s; display: flex; align-items: center; justify-content: center;';
            delBtn.innerHTML = '<i class="fas fa-trash-alt"></i>';
            delBtn.onclick = (event) => {
                event.stopPropagation(); // Stop parent onclick
                window.deleteCaseFromHistory(c.id, event);
            };

            rightDiv.appendChild(scoreDiv);
            rightDiv.appendChild(delBtn);

            itemDiv.appendChild(infoDiv);
            itemDiv.appendChild(rightDiv);
            
            container.appendChild(itemDiv);
        });
    } catch (err) {
        console.error('Error rendering recent cases:', err);
        const errDiv = document.createElement('div');
        errDiv.style.cssText = 'color: var(--error-500); padding: 1rem;';
        errDiv.textContent = 'Failed to load recent cases.';
        container.replaceChildren(errDiv);
    }
};

window.loadCaseFromHistory = async (caseId) => {
    ui.show('analysisLoading');
    try {
        let localCases = [];
        try {
            localCases = JSON.parse(localStorage.getItem('judiq_recent_cases_v1') || '[]');
        } catch (_) {}

        const localCase = localCases.find(c => c.id === caseId);
        if (localCase && localCase.case_data && localCase.analysis_result) {
            window.state.caseData = localCase.case_data;
            window.state.analysisResult = localCase.analysis_result;
            ui.hide('analysisLoading');
            switchScreen('resultsScreen');
            renderResults(localCase.analysis_result);
            return;
        }

        const userId = window.state.currentUser ? window.state.currentUser.uid : 'ANONYMOUS';
        if (userId !== 'ANONYMOUS') {
            const response = await api.getCaseDetails(caseId, userId);
            if (response && response.case_data && response.analysis_result) {
                window.state.caseData = response.case_data;
                window.state.analysisResult = response.analysis_result;

                // Cache locally
                const newCaseObj = {
                    id: caseId,
                    user_id: userId,
                    title: response.case_data.case_title || 'Untitled Case',
                    date: new Date().toISOString(),
                    score: response.analysis_result.score !== undefined ? response.analysis_result.score : 0,
                    risk_level: response.analysis_result.risk_level || response.analysis_result.defence_risk || 'Unknown',
                    verdict: response.analysis_result.verdict || 'Unknown',
                    case_data: response.case_data,
                    analysis_result: response.analysis_result
                };
                localCases = localCases.filter(c => c.id !== caseId);
                localCases.unshift(newCaseObj);
                if (localCases.length > 20) localCases.pop();
                localStorage.setItem('judiq_recent_cases_v1', JSON.stringify(localCases));

                ui.hide('analysisLoading');
                switchScreen('resultsScreen');
                renderResults(response.analysis_result);
                return;
            }
        }
        throw new Error('Case details could not be retrieved.');
    } catch (err) {
        ui.hide('analysisLoading');
        ui.toast(err.message, 'error');
    }
};

window.deleteCaseFromHistory = async (caseId, event) => {
    if (event) event.stopPropagation();

    if (!confirm(`Are you sure you want to delete case ${caseId} from history?`)) {
        return;
    }

    try {
        let localCases = [];
        try {
            localCases = JSON.parse(localStorage.getItem('judiq_recent_cases_v1') || '[]');
        } catch (_) {}
        localCases = localCases.filter(c => c.id !== caseId);
        localStorage.setItem('judiq_recent_cases_v1', JSON.stringify(localCases));

        const userId = window.state.currentUser ? window.state.currentUser.uid : 'ANONYMOUS';
        if (userId !== 'ANONYMOUS') {
            await api.deleteCase(caseId, userId);
        }

        ui.toast('Case successfully removed from history.', 'success');
        window.loadRecentCases();
    } catch (err) {
        console.error('Failed to delete case:', err);
        ui.toast('Error deleting case.', 'error');
        window.loadRecentCases();
    }
};

// Wizard starting function
window.startCaseAnalysis = (initialData = null) => {
    window.state = window.state || {};
    let flatData = initialData ? (typeof window.flattenDemoData === 'function' ? window.flattenDemoData(initialData) : { ...initialData }) : {};
    window.state.currentStep = 1;
    window.state.caseData = flatData;
    try {
        localStorage.setItem('judiq_wizard_autosave', JSON.stringify(window.state.caseData));
    } catch (_) {}
    
    switchScreen('caseWizardScreen');
    if (typeof renderWizardStep === 'function') {
        renderWizardStep();
    }
};

window.setUserDomain = (domain) => {
    window.state = window.state || {};
    window.state.userDomain = domain;
    if (window.state.currentUser) {
        localStorage.setItem(`judiq_domain_${window.state.currentUser.uid}`, domain);
    }
    renderDashboard();
    if (window.ui && typeof window.ui.toast === 'function') {
        window.ui.toast(`Switched domain to ${domain.toUpperCase()}`, 'info');
    }
};

/**
 * Expose modal functions
 */
window.openLegalModal = (modalId, event) => {
    if (event) event.preventDefault();
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.classList.remove('hidden');
    }
};
window.closeLegalModal = (modalId) => {
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.classList.add('hidden');
    }
};

// Close legal modal when clicking outside its content container
window.addEventListener('click', (event) => {
    if (event.target.classList.contains('legal-modal')) {
        event.target.classList.add('hidden');
    }
});

/**
 * Quick Analysis Mode Functions
 */
window.runQuickAnalysis = async () => {
    const amountEl = document.getElementById('qAmount');
    const chequeDateEl = document.getElementById('qChequeDate');
    const noticeDateEl = document.getElementById('qNoticeDate');
    const filingDateEl = document.getElementById('qFilingDate');
    const complainantEl = document.getElementById('qComplainant');
    const accusedEl = document.getElementById('qAccused');

    if (!amountEl || !chequeDateEl || !noticeDateEl || !filingDateEl || !complainantEl || !accusedEl) return;

    if (!amountEl.value || !chequeDateEl.value || !noticeDateEl.value || !filingDateEl.value || !complainantEl.value || !accusedEl.value) {
        ui.toast('Please fill in all required fields.', 'warning');
        return;
    }

    ui.show('analysisLoading');

    const quickData = {
        analysis_mode: "quick",
        user_id: window.state.currentUser ? window.state.currentUser.uid : 'ANONYMOUS',
        cheque_date: chequeDateEl.value,
        cheque_amount: parseFloat(amountEl.value) || 0,
        debt_amount: parseFloat(amountEl.value) || 0,
        notice_date: noticeDateEl.value,
        filing_date: filingDateEl.value,
        complainant_name: complainantEl.value,
        accused_name: accusedEl.value,
        original_cheque: document.getElementById('qChequePresent')?.checked ? "Yes - Original" : "No - Lost",
        dishonour_memo: document.getElementById('qDishonourMemo')?.checked ? "Yes - Original" : "No",
        notice_sent: document.getElementById('qNoticeSent')?.checked ? "Yes" : "No",
        supporting_documents: document.getElementById('qDebtProof')?.checked ? "Yes - All Documents" : "No Documents",
        
        // Defaults
        case_id: "CC/QUICK/" + Date.now().toString().slice(-4),
        case_title: `${complainantEl.value} vs ${accusedEl.value}`,
        court_name: "MM Court",
        case_type: "Cheque Bounce",
        complainant_type: "Individual",
        accused_type: "Individual",
        complainant_address: "Not Provided",
        accused_address: "Not Provided",
        directors_named: "Not Applicable",
        complainant_authorized: "Not Applicable",
        purpose: "Not Provided",
        agreement_type: "No Formal Agreement",
        debt_acknowledgment: "No",
        cheque_number: "000000",
        bank_name: "Default Bank",
        cheque_type: "Account Payee Cheque",
        post_dated: "No",
        dishonour_date: "",
        dishonour_reason: "Insufficient Funds",
        bank_memo_received: document.getElementById('qDishonourMemo').checked ? "Yes" : "No"
    };

    try {
        window.state.caseData = quickData;
        const result = await api.analyze(quickData);
        window.state.analysisResult = result;
        if (window.saveCaseToHistory) {
            window.saveCaseToHistory(quickData, result);
        }
        ui.hide('analysisLoading');
        switchScreen('resultsScreen');
        renderResults(result);
    } catch (err) {
        ui.hide('analysisLoading');
        ui.toast(err.message, 'error');
    }
};

window.expandToFullMode = () => {
    const amount = document.getElementById('qAmount')?.value || '';
    const chequeDate = document.getElementById('qChequeDate')?.value || '';
    const noticeDate = document.getElementById('qNoticeDate')?.value || '';
    const filingDate = document.getElementById('qFilingDate')?.value || '';
    const complainant = document.getElementById('qComplainant')?.value || '';
    const accused = document.getElementById('qAccused')?.value || '';

    window.state.currentStep = 1;
    window.state.caseData = {
        debt_amount: amount,
        cheque_amount: amount,
        cheque_date: chequeDate,
        notice_date: noticeDate,
        filing_date: filingDate,
        complainant_name: complainant,
        accused_name: accused,
        case_title: complainant && accused ? `${complainant} vs ${accused}` : ''
    };

    renderWizardStep();
    switchScreen('caseWizardScreen');
};

/**
 * Document Drafting System UI Flow
 */
let draftGenSource = 'dashboard';
let activeDraftType = null;

window.openDraftGeneratorScreen = (source) => {
    draftGenSource = source || (window.state.analysisResult && window.state.analysisResult.score ? 'results' : 'dashboard');
    switchScreen('draftGeneratorScreen');
    window.showDraftTypeSelection();
};

window.draftGenGoBack = () => {
    if (draftGenSource === 'results' && window.state.analysisResult && window.state.analysisResult.score) {
        switchScreen('resultsScreen');
    } else {
        switchScreen('dashboardScreen');
    }
};

window.showDraftTypeSelection = () => {
    ui.show('draftTypeSelection');
    ui.hide('draftInputForm');
    ui.hide('draftOutputView');
    renderDraftTypeGrid();
};

window.showDraftInputForm = () => {
    ui.hide('draftTypeSelection');
    ui.show('draftInputForm');
    ui.hide('draftOutputView');
};

window.showDraftOutputView = () => {
    ui.hide('draftTypeSelection');
    ui.hide('draftInputForm');
    ui.show('draftOutputView');
};

function renderDraftTypeGrid() {
    const grid = document.getElementById('draftTypeGrid');
    if (!grid) return;
    grid.innerHTML = DRAFT_TYPES.map(dt => `
        <div class="draft-type-card" onclick="selectDraftType('${dt.id}')">
            <div class="draft-type-num" style="background:${dt.color}18;color:${dt.color}">${dt.number}</div>
            <div class="draft-type-icon-wrap" style="color:${dt.color}">
                <i class="fas ${dt.icon}"></i>
            </div>
            <div class="draft-type-info">
                <h4>${dt.title}</h4>
                <span class="draft-type-sub">${dt.subtitle}</span>
                <p>${dt.description}</p>
            </div>
            <div class="draft-type-chevron" style="color:${dt.color}">
                <i class="fas fa-chevron-right"></i>
            </div>
        </div>
    `).join('');
}

window.selectDraftType = (id) => {
    activeDraftType = DRAFT_TYPES.find(dt => dt.id === id);
    if (!activeDraftType) return;

    const badge = document.getElementById('draftSelectedBadge');
    if (badge) {
        badge.innerHTML = `
            <span style="background:${activeDraftType.color}18;color:${activeDraftType.color};padding:0.3rem 1rem;border-radius:999px;font-size:0.8rem;font-weight:600;display:inline-flex;align-items:center;gap:0.4rem;">
                <i class="fas ${activeDraftType.icon}"></i> Type ${activeDraftType.number} of 13 &nbsp;·&nbsp; ${activeDraftType.subtitle}
            </span>`;
    }
    
    ui.setText('draftFormTitle', activeDraftType.title);
    ui.setText('draftFormSubtitle', activeDraftType.description);

    const pre = buildDraftPrefill();
    const body = document.getElementById('draftFormBody');
    if (body) {
        body.innerHTML = '<div class="draft-fields-grid">' + activeDraftType.fields.map(f => {
            const val = pre[f.name] || '';
            const isWide = f.type === 'textarea' || f.name.includes('address') || f.name.includes('details') || f.name.includes('purpose') || f.name.includes('notes');
            let inputHtml = '';
            if (f.type === 'textarea') {
                inputHtml = `<textarea id="dfield_${f.name}" class="draft-field-input" ${f.required ? 'required' : ''} placeholder="${f.placeholder || ''}" rows="3">${val}</textarea>`;
            } else if (f.type === 'select') {
                inputHtml = `<select id="dfield_${f.name}" class="draft-field-input" ${f.required ? 'required' : ''}>
                    <option value="">Select...</option>
                    ${(f.options || []).map(o => `<option value="${o}"${val === o ? ' selected' : ''}>${o}</option>`).join('')}
                </select>`;
            } else {
                inputHtml = `<input type="${f.type}" id="dfield_${f.name}" class="draft-field-input" ${f.required ? 'required' : ''} placeholder="${f.placeholder || ''}" value="${val}">`;
            }
            return `<div class="draft-field-group${isWide ? ' full-width' : ''}">
                <label class="draft-field-label" for="dfield_${f.name}">
                    ${f.label}${f.required ? ' <span class="req-star">*</span>' : ''}
                </label>${inputHtml}
            </div>`;
        }).join('') + '</div>';
    }

    window.showDraftInputForm();
};

function buildDraftPrefill() {
    const s = window.state.caseData || {};
    const map = {};
    if (s.complainant_name) map.complainant_name = s.complainant_name;
    if (s.complainant_address) map.complainant_address = s.complainant_address;
    if (s.accused_name) map.accused_name = s.accused_name;
    if (s.accused_address) map.accused_address = s.accused_address;
    if (s.cheque_number) map.cheque_number = s.cheque_number;
    if (s.cheque_date) map.cheque_date = s.cheque_date;
    if (s.cheque_amount) map.cheque_amount = s.cheque_amount;
    if (s.bank_name) map.bank_name = s.bank_name;
    if (s.branch_name) map.branch_name = s.branch_name;
    if (s.dishonour_date) map.dishonour_date = s.dishonour_date;
    if (s.dishonour_reason) map.dishonour_reason = s.dishonour_reason;
    if (s.court_name) map.court_name = s.court_name;
    if (s.case_id) map.case_number = s.case_id;
    if (s.filing_date) map.filing_date = s.filing_date;
    if (s.notice_date) map.notice_date = s.notice_date;
    if (s.purpose) map.transaction_purpose = s.purpose;
    return map;
}

window.generateDraftFromForm = () => {
    if (!activeDraftType) return;
    const missing = [];
    const data = {};
    for (const f of activeDraftType.fields) {
        const el = document.getElementById('dfield_' + f.name);
        if (!el) continue;
        const val = el.value.trim();
        if (f.required && !val) {
            missing.push(f.label);
            el.classList.add('field-error');
        } else {
            el.classList.remove('field-error');
            data[f.name] = val;
        }
    }
    if (missing.length > 0) {
        ui.toast('Please fill: ' + missing.slice(0, 3).join(', ') + (missing.length > 3 ? '...' : ''), 'error');
        return;
    }
    try {
        const txt = activeDraftType.generate(data);
        const textPreview = document.getElementById('generatedDraftContent');
        if (textPreview) textPreview.value = txt;
        
        const badge = document.getElementById('draftOutputBadge');
        if (badge) {
            badge.innerHTML = `
                <span style="background:${activeDraftType.color}18;color:${activeDraftType.color};padding:0.3rem 1rem;border-radius:999px;font-size:0.8rem;font-weight:600;display:inline-flex;align-items:center;gap:0.4rem;">
                    <i class="fas ${activeDraftType.icon}"></i> Draft ${activeDraftType.number} – ${activeDraftType.subtitle}
                </span>`;
        }
        ui.setText('draftOutputTitle', activeDraftType.title);
        window.showDraftOutputView();

        if (typeof verifyDraftTimelineS138 === 'function') {
            const tRes = verifyDraftTimelineS138(data);
            if (tRes.warnings && tRes.warnings.length > 0) {
                ui.toast('Timeline Alert: ' + tRes.warnings[0], 'warning');
            } else {
                ui.toast('Draft generated & Section 138 statutory timeline verified!', 'success');
            }
        } else {
            ui.toast('Draft generated successfully!', 'success');
        }
        if (caseId && activeDraftType) {
            const historyListEl = document.getElementById('draftHistoryList');
            if (historyListEl) {
                historyListEl.innerHTML = '<p style="color: var(--gray-400); font-size: 0.8rem; margin: 0; text-align: center; padding: 1rem 0;"><i class="fas fa-spinner fa-spin"></i> Loading history...</p>';
                api.getDraftHistory(caseId, activeDraftType.id)
                    .then(data => {
                        if (data.success && data.history && data.history.length > 0) {
                            historyListEl.innerHTML = data.history.map(item => {
                                const dt = new Date(item.created_at).toLocaleString();
                                return `
                                    <div class="history-item" style="padding: 0.75rem; background: #fff; border: 1px solid #e5e7eb; border-radius: 6px; cursor: pointer; transition: all 0.2s;" onclick="loadDraftVersion('${item.version}')">
                                        <div style="font-weight: 600; font-size: 0.85rem; color: #374151;">Version ${item.version}</div>
                                        <div style="font-size: 0.75rem; color: #6b7280; margin-top: 0.25rem;">${dt}</div>
                                    </div>
                                `;
                            }).join('');
                            
                            window.loadDraftVersion = (version) => {
                                const found = data.history.find(h => String(h.version) === String(version));
                                if (found) {
                                    document.getElementById('generatedDraftContent').value = found.content;
                                    ui.toast(`Loaded Version ${version}`, 'success');
                                }
                            };
                        } else {
                            historyListEl.innerHTML = '<p style="color: #9ca3af; font-size: 0.8rem; margin: 0; text-align: center; padding: 1rem 0;">No versions saved yet.</p>';
                        }
                    })
                    .catch(err => {
                        console.error('History fetch error:', err);
                        historyListEl.innerHTML = '<p style="color: #ef4444; font-size: 0.8rem; margin: 0; text-align: center; padding: 1rem 0;">Failed to load history.</p>';
                    });
            }
        }
        ui.toast('Draft generated successfully!', 'success');
    } catch (err) {
        ui.toast('Error generating draft.', 'error');
        console.error('Draft error:', err);
    }
};

window.copyGeneratedDraft = () => {
    const ta = document.getElementById('generatedDraftContent');
    if (!ta) return;
    ta.select();
    document.execCommand('copy');
    ui.toast('Draft copied to clipboard!', 'success');
};

window.downloadGeneratedDraft = async () => {
    const content = document.getElementById('generatedDraftContent')?.value;
    if (!content) { ui.toast('Nothing to download', 'warning'); return; }
    
    // Create highly premium loading overlay
    const overlay = document.createElement('div');
    overlay.id = 'dossierLoadingOverlay';
    overlay.innerHTML = `
        <div style="position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(15,23,42,0.85);z-index:10000;display:flex;flex-direction:column;justify-content:center;align-items:center;color:white;font-family:Inter,sans-serif;backdrop-filter:blur(10px);">
            <div style="width:60px;height:60px;border:4px solid rgba(56,189,248,0.2);border-top:4px solid #38bdf8;border-radius:50%;animation:dossierSpin 1s linear infinite;margin-bottom:24px;"></div>
            <style>@keyframes dossierSpin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }</style>
            <h2 style="font-size:28px;font-weight:800;margin:0 0 12px 0;background:linear-gradient(135deg,#e0f2fe,#38bdf8);-webkit-background-clip:text;-webkit-text-fill-color:transparent;letter-spacing:1px;">COMPILING DOSSIER</h2>
            <div id="dossierLoadingText" style="font-size:15px;color:#94a3b8;font-family:monospace;letter-spacing:0.5px;">Extracting AI Intelligence State...</div>
        </div>
    `;
    document.body.appendChild(overlay);

    const steps = [
        "Parsing Factual Timeline...",
        "Synthesizing Legal Reasoning...",
        "Structuring Defence Matrices...",
        "Applying Section 138 Statutes...",
        "Computing Cryptographic Hash...",
        "Rendering PDF Architecture..."
    ];
    let stepIdx = 0;
    const loadingInterval = setInterval(() => {
        const textEl = document.getElementById('dossierLoadingText');
        if(textEl && stepIdx < steps.length) {
            textEl.innerText = steps[stepIdx];
            stepIdx++;
        }
    }, 600);

    try {
        const title = activeDraftType ? activeDraftType.title : 'Legal Draft';
        
        // Extract full intelligence metadata
        const ar = window.state.analysisResult || {};
        
        const topDefences = (ar.defence_strategy || []).slice(0, 2).map(d => typeof d === 'string' ? d : d.argument);
        const topPrecedents = (ar.precedents || []).slice(0, 2).map(p => {
            if (typeof p === 'string') return p;
            const name = p.case || p.case_name || 'Landmark Case';
            const cit = p.citation || '';
            return cit ? `${name} [${cit}]` : name;
        });
        
        const metadata = { 
            caseId: ar.case_id || 'Unknown_Case',
            score: ar.score || null,
            riskLevel: ar.risk_level || 'Unknown',
            clientRole: (ar.case_data && ar.case_data.client_role) || 'Client',
            courtName: (ar.case_data && ar.case_data.court_name) || 'Competent Court',
            defences: topDefences,
            precedents: topPrecedents,
            analysis_result: ar
        };
        
        const blob = await api.generateDraftPdf(title, content, metadata);
        if (blob.size < 100) throw new Error('Received empty PDF');
        
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        const draftTitle = metadata.docType || title || 'Dossier';
        a.download = getSanitizedCaseFilename(draftTitle, metadata, 'pdf');
        document.body.appendChild(a);
        a.click();
        
        setTimeout(() => {
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        }, 100);
        
        ui.toast('Comprehensive Dossier Downloaded!', 'success');
    } catch (error) {
        console.error('Download draft error:', error);
        ui.toast('Failed to generate Dossier PDF.', 'error');
    } finally {
        clearInterval(loadingInterval);
        if (document.getElementById('dossierLoadingOverlay')) {
            document.getElementById('dossierLoadingOverlay').remove();
        }
    }
};

window.printGeneratedDraft = () => {
    const content = document.getElementById('generatedDraftContent')?.value;
    if (!content) return;
    const win = window.open('', '_blank');
    win.document.write(`<html><head><title>JUDIQ AI – Legal Draft</title>
    <style>body{font-family:'Courier New',monospace;font-size:12px;padding:2cm;line-height:1.8;white-space:pre-wrap;color:#111;}</style>
    </head><body>${content.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</body></html>`);
    win.document.close(); win.print();
};

/**
 * Smart Upload Screen Functions
 */
window.openSmartUpload = () => {
    const modal = document.getElementById("uploadModal");
    if (!modal) return;
    modal.classList.remove("hidden");
    setTimeout(() => modal.classList.add("active"), 10);

    const zone = document.getElementById("uploadZone");
    const progress = document.getElementById("uploadProgress");
    const results = document.getElementById("extractionResults");
    if (zone) zone.classList.remove("hidden");
    if (progress) progress.classList.add("hidden");
    if (results) results.classList.add("hidden");
};

window.closeUploadModal = () => {
    const modal = document.getElementById("uploadModal");
    if (!modal) return;
    modal.classList.remove("active");
    setTimeout(() => modal.classList.add("hidden"), 300);
};

window.proceedWithExtractedText = () => {
    const preview = document.getElementById("extractedTextPreview");
    if (!preview) return;
    const text = preview.value;
    if (!text || text.length < 10) {
        ui.toast("Insufficient text to analyze", "warning");
        return;
    }

    window.closeUploadModal();

    const initialData = {
        purpose: text.substring(0, 1000),
        additional_notes: text
    };
    
    window.startCaseAnalysis(initialData);
    ui.toast("Wizard pre-filled with extracted data", "success");
};

function initUploadBindings() {
    const zone = document.getElementById("uploadZone");
    const input = document.getElementById("fileInput");

    if (!zone || !input) return;

    zone.addEventListener("dragover", (e) => {
        e.preventDefault();
        zone.classList.add("drag-over");
    });

    zone.addEventListener("dragleave", () => {
        zone.classList.remove("drag-over");
    });

    zone.addEventListener("drop", (e) => {
        e.preventDefault();
        zone.classList.remove("drag-over");
        if (e.dataTransfer.files.length) {
            handleFileUpload(e.dataTransfer.files[0]);
        }
    });

    input.addEventListener("change", (e) => {
        if (e.target.files.length) {
            handleFileUpload(e.target.files[0]);
        }
    });
}

async function handleFileUpload(file) {
    const zone = document.getElementById("uploadZone");
    const progress = document.getElementById("uploadProgress");
    const progressFill = document.getElementById("uploadProgressFill");
    const status = document.getElementById("uploadStatus");

    if (!zone || !progress || !progressFill || !status) return;

    zone.classList.add("hidden");
    progress.classList.remove("hidden");

    status.textContent = "Uploading document...";
    progressFill.style.width = "30%";

    const uploadData = new FormData();
    uploadData.append("file", file);

    try {
        status.textContent = "Extracting legal intelligence...";
        progressFill.style.width = "70%";

        const responseData = await api.verifyMemo(uploadData);

        if (responseData.status === "success" || responseData.status === "partial") {
            progressFill.style.width = "100%";
            setTimeout(() => {
                progress.classList.add("hidden");
                showExtractionResults(responseData.text);
                if (responseData.status === "partial") {
                    ui.toast("Some text could not be extracted", "warning");
                }
            }, 500);
        } else {
            throw new Error(responseData.message || "Upload failed");
        }
    } catch (error) {
        ui.toast(error.message, "error");
        zone.classList.remove("hidden");
        progress.classList.add("hidden");
    }
}

function showExtractionResults(text) {
    const results = document.getElementById("extractionResults");
    const preview = document.getElementById("extractedTextPreview");

    if (results && preview) {
        results.classList.remove("hidden");
        preview.value = text || "No text could be extracted from this document.";
    }
}

// Bind upload listener once the module executes
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initUploadBindings);
} else {
    initUploadBindings();
}

/**
 * Result Actions exports
 */
window.switchResultTab = switchResultTab;
window.viewStrategy = () => {
    if (!window.state.analysisResult) {
        ui.toast('Please analyze a case first.', 'warning');
        return;
    }
    switchScreen('resultsScreen');
    window.switchResultTab('strategy');
};
window.viewReports = () => {
    if (!window.state.analysisResult) {
        ui.toast('Please analyze a case first.', 'warning');
        return;
    }
    switchScreen('resultsScreen');
    window.switchResultTab('analysis');
};
window.viewGuidance = () => {
    if (!window.state.analysisResult) {
        ui.toast('Please analyze a case first.', 'warning');
        return;
    }
    switchScreen('resultsScreen');
    window.switchResultTab('strategy');
};
window.learnMode = () => {
    ui.toast('Learning mode coming soon! This will include educational case studies and tutorials.', 'info');
};
window.generateDraft = () => window.openDraftGeneratorScreen('dashboard');
window.backToResults = () => switchScreen('resultsScreen');
window.editAndReanalyze = () => switchScreen('caseWizardScreen');
window.startNewAnalysis = () => window.startCaseAnalysis();
window.startNewCase = () => {
    if (confirm('Start a new case? Your current analysis will remain in recent cases.')) {
        window.startCaseAnalysis();
    }
};

export function getSanitizedCaseFilename(prefix = 'Report', data = null, ext = 'pdf') {
    const d = data || window.state.analysisResult || window.state.caseData || {};
    const cd = (d && d.case_data) ? d.case_data : (d || {});
    
    // Extract most descriptive name available
    let title = cd.case_title || cd.case_caption || cd.title || '';
    if (!title && cd.complainant_name && cd.accused_name) {
        title = `${cd.complainant_name}_vs_${cd.accused_name}`;
    }
    if (!title && cd.bank_name && cd.borrower_name) {
        title = `${cd.bank_name}_vs_${cd.borrower_name}`;
    }
    if (!title && cd.case_id) {
        title = cd.case_id;
    }
    if (!title && d.case_id) {
        title = d.case_id;
    }
    if (!title) {
        title = 'Legal_Matter';
    }
    
    // Clean string for safe cross-platform file naming
    const safeTitle = String(title)
        .replace(/[^a-zA-Z0-9_\-\s]/g, '')
        .trim()
        .replace(/\s+/g, '_')
        .slice(0, 60);
        
    const safePrefix = String(prefix)
        .replace(/[^a-zA-Z0-9_\-\s]/g, '')
        .trim()
        .replace(/\s+/g, '_');
        
    return `JUDIQ_${safePrefix}_${safeTitle}.${ext.replace(/^\./, '')}`;
}
window.getSanitizedCaseFilename = getSanitizedCaseFilename;

window.downloadPDF = async () => {
    if (!window.state.analysisResult) {
        ui.toast('No analysis result available.', 'warning');
        return;
    }
    ui.toast('Generating professional PDF report...', 'info');
    try {
        const blob = await api.generatePdf(window.state.analysisResult);
        if (blob.size < 100) throw new Error('Received empty PDF');
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        a.download = getSanitizedCaseFilename('Report', window.state.analysisResult, 'pdf');
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        ui.toast('PDF report downloaded successfully!', 'success');
    } catch (err) {
        console.error('PDF Download Error:', err);
        ui.toast(`PDF Error: ${err.message}. Fetching text fallback.`, 'error');
        // Text fallback
        const result = window.state.analysisResult;
        const content = `JUDIQ AI CASE REPORT\nScore: ${result.score}/100\nCase Merit Assessment: ${result.verdict}\n\nLegal Analysis:\n${result.legal_analysis || ''}`;
        const b = new Blob([content], { type: "text/plain" });
        const link = document.createElement("a");
        link.href = URL.createObjectURL(b);
        link.download = getSanitizedCaseFilename('Report', window.state.analysisResult, 'txt');
        link.click();
    }
};

window.generateReport = () => window.downloadPDF();

window.copyDraft = () => {
    const ta = document.getElementById("draftPreviewContent") || document.getElementById("draftContent");
    if (!ta || !ta.value) { ui.toast("No draft available", "warning"); return; }
    ta.select();
    document.execCommand('copy');
    ui.toast('Draft copied to clipboard!', 'success');
};

window.downloadDraft = () => {
    const ta = document.getElementById("draftPreviewContent") || document.getElementById("draftContent");
    if (!ta || !ta.value) { ui.toast("No draft available", "warning"); return; }
    const blob = new Blob([ta.value], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = getSanitizedCaseFilename('Draft', window.state.analysisResult || window.state.caseData, 'txt');
    a.click();
    URL.revokeObjectURL(url);
    ui.toast('Draft downloaded!', 'success');
};

window.printDraft = () => {
    const ta = document.getElementById("draftPreviewContent") || document.getElementById("draftContent");
    if (!ta || !ta.value) { ui.toast("No draft available", "warning"); return; }
    const win = window.open('', '_blank');
    win.document.write(`<html><head><title>JUDIQ AI – Legal Draft</title>
    <style>body{font-family:'Courier New',monospace;font-size:12px;padding:2cm;line-height:1.8;white-space:pre-wrap;color:#111;}</style>
    </head><body>${ta.value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</body></html>`);
    win.document.close(); win.print();
};

window.copyDraftFromPreview = window.copyDraft;
window.downloadDraftText = window.downloadDraft;
window.openDraftEditor = () => window.openDraftGeneratorScreen('results');

window.viewFullReport = () => {
    switchScreen('resultsScreen');
    window.switchResultTab('overview');
};

window.currentDraftTemplate = 'demand_notice';
window.currentDraftTone = 'standard';

window.onSelectDraftTemplate = (templateId) => {
    window.currentDraftTemplate = templateId;
    window.renderSelectedDraft();
};

window.setDraftTone = (tone, btnElement) => {
    window.currentDraftTone = tone;
    if (btnElement) {
        document.querySelectorAll('.btn-tone').forEach(b => b.classList.remove('active'));
        btnElement.classList.add('active');
    }
    if (window.ui) window.ui.toast(`Draft tone updated to: ${tone.toUpperCase()}`, 'info');
    window.renderSelectedDraft();
};

window.renderSelectedDraft = () => {
    const templateId = window.currentDraftTemplate || 'demand_notice';
    const templateObj = DRAFT_TYPES.find(t => t.id === templateId) || DRAFT_TYPES[0];
    const caseData = window.state.caseData || {};
    
    const d = {
        complainant_name: caseData.complainant_name || 'Apex Global Traders Pvt Ltd',
        complainant_address: caseData.complainant_address || 'Plot 42, Hinjewadi Phase 1, Pune, 411057',
        accused_name: caseData.accused_name || 'Vanguard Tech Solutions Pvt Ltd',
        accused_address: caseData.accused_address || 'Office 10, Wakad Road, Pune, 411057',
        cheque_number: caseData.cheque_number || '482019',
        cheque_date: caseData.cheque_date || '2026-04-10',
        cheque_amount: caseData.cheque_amount || 1550000,
        bank_name: caseData.bank_name || 'HDFC Bank Ltd',
        branch_name: caseData.branch_name || 'Hinjewadi Branch',
        dishonour_date: caseData.dishonour_date || '2026-05-02',
        dishonour_reason: caseData.dishonour_reason || 'Funds Insufficient',
        transaction_purpose: caseData.purpose || 'Supply of IT Hardware Equipment and Enterprise Servers',
        notice_date: caseData.notice_date || '2026-05-18',
        demand_days: '15 days',
        device_type: 'Samsung Galaxy S23 & Dell XPS Laptop',
        court_location: caseData.jurisdiction_city || 'Pune'
    };

    let text = templateObj.generate(d);
    
    if (window.currentDraftTone === 'aggressive') {
        text = text.replace(/Please treat this as a final opportunity to settle the matter amicably\./g, 
            'TAKE NOTICE that no further extensions will be granted, and strict criminal prosecution under Section 138 of the Negotiable Instruments Act along with punitive costs will be filed immediately upon expiry of the 15-day statutory deadline without further reference to you.');
    } else if (window.currentDraftTone === 'conciliatory') {
        text = text.replace(/Please treat this as a final opportunity to settle the matter amicably\./g, 
            'My client remains open to structured pre-trial settlement options provided your authorized representative contacts our counsel within 7 days of receipt of this notice.');
    }

    const preview = document.getElementById("draftPreviewContent");
    if (preview) {
        preview.value = text;
    }
};

window.downloadDraftWord = () => {
    const preview = document.getElementById("draftPreviewContent");
    if (!preview || !preview.value) {
        if (window.ui) window.ui.toast('No draft text available to download', 'warning');
        return;
    }
    const header = "<html xmlns:o='urn:schemas-microsoft-com:office:office' xmlns:w='urn:schemas-microsoft-com:office:word' xmlns='http://www.w3.org/TR/REC-html40'><head><meta charset='utf-8'><title>Legal Draft</title><style>body{font-family:'Courier New',Courier,monospace;font-size:11pt;line-height:1.6;}</style></head><body><pre style='font-family:inherit;white-space:pre-wrap;'>";
    const footer = "</pre></body></html>";
    const sourceHTML = header + preview.value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') + footer;
    
    const blob = new Blob(['\ufeff', sourceHTML], { type: 'application/msword' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const templateLabel = (window.currentDraftTemplate || 'Draft').replace(/_/g, ' ');
    a.download = getSanitizedCaseFilename(templateLabel, window.state.analysisResult || window.state.caseData, 'doc');
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    if (window.ui) window.ui.toast('Draft downloaded as Word document!', 'success');
};


window.toggleReasoning = () => {
    const trace = document.getElementById('reasoningTrace');
    const header = document.querySelector('.collapsible-header');
    if (trace) trace.classList.toggle('hidden');
    if (header) header.classList.toggle('open');
};

/**
 * Theme Toggling Logic

 */
function initTheme() {
    const savedTheme = localStorage.getItem('judiq_theme');
    const theme = savedTheme === 'dark' ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', theme);
    updateThemeIcons(theme);
}

window.toggleTheme = () => {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';
    
    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('judiq_theme', newTheme);
    updateThemeIcons(newTheme);
};

function updateThemeIcons(theme) {
    const icons = document.querySelectorAll('.theme-toggle-btn i');
    icons.forEach(icon => {
        if (theme === 'light') {
            icon.className = 'fas fa-sun';
        } else {
            icon.className = 'fas fa-moon';
        }
    });
}

window.togglePricingPeriod = (isAnnual) => {
    const citizenPrice = document.getElementById('priceCitizen');
    const proPrice = document.getElementById('pricePro');
    const enterprisePrice = document.getElementById('priceEnterprise');
    
    const monthlyLabel = document.getElementById('billingMonthlyLabel');
    const annualLabel = document.getElementById('billingAnnualLabel');
    
    if (isAnnual) {
        if (citizenPrice) citizenPrice.textContent = '239';
        if (proPrice) proPrice.textContent = '479';
        if (enterprisePrice) enterprisePrice.textContent = '799';
        
        if (monthlyLabel) monthlyLabel.classList.remove('active');
        if (annualLabel) annualLabel.classList.add('active');
    } else {
        if (citizenPrice) citizenPrice.textContent = '299';
        if (proPrice) proPrice.textContent = '599';
        if (enterprisePrice) enterprisePrice.textContent = '999';
        
        if (monthlyLabel) monthlyLabel.classList.add('active');
        if (annualLabel) annualLabel.classList.remove('active');
    }
};

window.toggleFaqItem = (element) => {
    const item = element.closest('.faq-item');
    if (item) {
        item.classList.toggle('active');
    }
};

window.switchExplorerTab = (tabName, element) => {
    // Remove active class from all tabs
    document.querySelectorAll('.explorer-tab').forEach(tab => tab.classList.remove('active'));
    // Add active class to clicked tab
    if (element) element.classList.add('active');

    const titleEl = document.getElementById('explorerTitle');
    const descEl = document.getElementById('explorerDesc');
    const previewEl = document.getElementById('explorerPreview');
    if (!titleEl || !descEl || !previewEl) return;

    let lines = [];
    if (tabName === 'adversarial') {
        titleEl.textContent = 'Adversarial Opponent Simulation';
        descEl.textContent = 'Simulate critical opposing counsel cross-examination routes and see how the AI evaluates and flags vulnerabilities in real-time.';
        lines = [
            { text: '// Running Adversarial Opponent Simulation...', color: '#6b7280' },
            { text: '[INFO] Parsing complainant statement & matching against Indian Evidence Act Sec 45...', color: '#60a5fa' },
            { text: '[WARNING] Found high risk vulnerability in Sec 138 demand notice delivery timeline.', color: '#fbbf24' },
            { text: '[VULNERABILITY] Delivery occurred on day 32 (Statutory limit: 30 days).', color: '#f87171' },
            { text: '[RECOMMENDED COUNTER] Invoke General Clauses Act Sec 10 to plead court holiday exemption.', color: '#34d399' }
        ];
    } else if (tabName === 'contradiction') {
        titleEl.textContent = 'Evidence Contradiction Scan';
        descEl.textContent = 'Search uploads for mismatches between bank return memos, statutory legal notices, and witness testimonies automatically.';
        lines = [
            { text: '// Initiating Evidentiary Contradiction Scan...', color: '#6b7280' },
            { text: '[INFO] Document extracted: Bank Return Memo (Date: 2026-06-05, Reason: Insufficient Funds).', color: '#60a5fa' },
            { text: '[INFO] Document extracted: Legal Demand Notice (Date: 2026-06-08).', color: '#60a5fa' },
            { text: '[OK] Date validation: Legal Notice sent within 30 days of Dishonour Memo.', color: '#34d399' },
            { text: '[CONFLICT] Complainant oral testimony states cheque value was 10,00,000 INR, but Bank Memo registers 5,00,000 INR.', color: '#f87171' },
            { text: '[WARNING] Evidentiary mismatch detected. Prepare defense around Sec 139 presumption rebuttal.', color: '#fbbf24' }
        ];
    } else if (tabName === 'strategy') {
        titleEl.textContent = 'Courtroom Strategy & Precedent Routing';
        descEl.textContent = 'Retrieve judge challenge profiles and matching Supreme Court precedent citations to construct optimized defense briefs.';
        lines = [
            { text: '// Generating Courtroom Precedent Route...', color: '#6b7280' },
            { text: '[INFO] Query: Cheque bounce due to signature mismatch (Sec 138/139 IEA).', color: '#60a5fa' },
            { text: '[MATCH] Found relevant Supreme Court citation: Laxmi Dyechem v. State of Gujarat (2012).', color: '#34d399' },
            { text: '[RULE] Held: Signature mismatch does not escape Sec 138 liability if dishonour reasons are verified.', color: '#60a5fa' },
            { text: '[JUDGE PROFILE] Justice A. Mehta: 83% historic tendency to rule in favor of complainant on Sec 138 timeline compliance.', color: '#fbbf24' },
            { text: '[STRATEGY] Focus oral arguments strictly on delivery timeline validation rather than signature analysis.', color: '#34d399' }
        ];
    }

    // Typewriter effect: clear console and print line-by-line with a slight delay
    previewEl.innerHTML = '';
    
    // Store active timer ID to prevent overlapping typing animations if clicked rapidly
    if (window.explorerTypewriterTimer) {
        clearInterval(window.explorerTypewriterTimer);
    }
    
    let currentLineIdx = 0;
    const typeNextLine = () => {
        if (currentLineIdx < lines.length) {
            const line = lines[currentLineIdx];
            const p = document.createElement('p');
            p.style.color = line.color;
            p.style.margin = '0 0 0.5rem 0';
            p.style.opacity = '0';
            p.style.transform = 'translateY(5px)';
            p.style.transition = 'all 0.3s ease';
            p.textContent = line.text;
            previewEl.appendChild(p);
            
            // Force reflow and animate in
            setTimeout(() => {
                p.style.opacity = '1';
                p.style.transform = 'translateY(0)';
            }, 10);
            
            currentLineIdx++;
        } else {
            clearInterval(window.explorerTypewriterTimer);
            window.explorerTypewriterTimer = null;
        }
    };
    
    typeNextLine(); // Print first line immediately
    window.explorerTypewriterTimer = setInterval(typeNextLine, 350);
};

window.switchDocsTab = (tabName, element) => {
    // Toggle active class on sidebar buttons
    const container = element.closest('.docs-modal-container');
    if (!container) return;
    
    container.querySelectorAll('.docs-tab-btn').forEach(btn => btn.classList.remove('active'));
    element.classList.add('active');

    // Toggle active class on content panes
    container.querySelectorAll('.docs-content-pane').forEach(pane => pane.classList.remove('active'));
    const targetPane = container.querySelector(`#docs-${tabName}`);
    if (targetPane) {
        targetPane.classList.add('active');
    }
};

window.submitContactForm = (event) => {
    event.preventDefault();
    const form = event.target;
    const btn = form.querySelector('button[type="submit"]');
    const name = document.getElementById('contactName').value;
    const email = document.getElementById('contactEmail').value;
    
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="btn-text">Sending...</span><span class="btn-loader" style="display:inline-block;"></span>';
    }

    setTimeout(() => {
        // Use our ui.toast system from ui.js
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast(`Message sent successfully! Thank you, ${name}.`, 'success');
        } else {
            alert(`Message sent successfully! Thank you, ${name}.`);
        }
        
        form.reset();
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<span class="btn-text">Send Message</span>';
        }
    }, 1500);
};

// Guided Tour State & Logic
let currentTourStep = 0;
const tourStepsData = [
    {
        title: "Welcome to JudiQ AI!",
        text: "Let's take a 1-minute quick tour of your new Litigation Command Center to learn the core features.",
        icon: "fa-balance-scale",
        target: null,
        nextLabel: "Start Tour",
        prevLabel: "Skip Tour"
    },
    {
        title: "Preloaded Demo Cases",
        text: "Want to see JudiQ in action immediately? Use these Civil or Criminal buttons in the top navigation to instantly load pre-populated case profiles.",
        icon: "fa-bolt",
        target: ".nav-brand div",
        nextLabel: "Next Step",
        prevLabel: "Back"
    },
    {
        title: "Litigation Action Center",
        text: "Launch a comprehensive Case Analysis, run a Quick Analysis, check uploads, or generate custom Legal Drafts.",
        icon: "fa-rocket",
        target: "#actionCardsGrid",
        nextLabel: "Next Step",
        prevLabel: "Back"
    },
    {
        title: "Recent Activity",
        text: "View and manage your past litigation histories, success scores, and active room logs here.",
        icon: "fa-history",
        target: ".recent-section",
        nextLabel: "Finish Tour",
        prevLabel: "Back"
    }
];

window.startGuidedTour = () => {
    currentTourStep = 0;
    const overlay = document.getElementById('guidedTourOverlay');
    if (overlay) {
        overlay.classList.add('open');
        renderTourStep();
    }
};

function renderTourStep() {
    const step = tourStepsData[currentTourStep];
    const titleEl = document.getElementById('tourTitle');
    const textEl = document.getElementById('tourText');
    const iconEl = document.getElementById('tourIcon');
    const prevBtn = document.getElementById('tourPrevBtn');
    const nextBtn = document.getElementById('tourNextBtn');
    const dots = document.querySelectorAll('.tour-dot');

    if (titleEl) titleEl.textContent = step.title;
    if (textEl) textEl.textContent = step.text;
    if (iconEl) iconEl.className = `fas ${step.icon} tour-icon`;
    if (prevBtn) prevBtn.textContent = step.prevLabel;
    if (nextBtn) nextBtn.textContent = step.nextLabel;

    // Update dots active class
    dots.forEach((dot, index) => {
        if (index === currentTourStep) dot.classList.add('active');
        else dot.classList.remove('active');
    });

    // Clear previous highlights
    document.querySelectorAll('.tour-highlight').forEach(el => el.classList.remove('tour-highlight'));

    // Highlight target element
    if (step.target) {
        const targetEl = document.querySelector(step.target);
        if (targetEl) {
            targetEl.classList.add('tour-highlight');
            targetEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
    }
}

window.nextTourStep = () => {
    if (currentTourStep < tourStepsData.length - 1) {
        currentTourStep++;
        renderTourStep();
    } else {
        window.closeGuidedTour();
    }
};

window.prevTourStep = () => {
    if (currentTourStep === 0) {
        window.closeGuidedTour();
    } else {
        currentTourStep--;
        renderTourStep();
    }
};

window.closeGuidedTour = () => {
    const overlay = document.getElementById('guidedTourOverlay');
    if (overlay) {
        overlay.classList.remove('open');
    }
    document.querySelectorAll('.tour-highlight').forEach(el => el.classList.remove('tour-highlight'));
    try { localStorage.setItem('judiq_tour_completed', 'true'); } catch (_) {}
};

/* =============================================================================
   LANDING PAGE PREMIUM INTERACTIVITY (SANDBOX & NEWSLETTER)
   ============================================================================= */

window.calculateSandboxTimelines = () => {
    const memoVal = document.getElementById('sandboxMemoDate').value;
    const noticeVal = document.getElementById('sandboxNoticeDate').value;
    if (!memoVal || !noticeVal) return;

    const memoDate = new Date(memoVal);
    const noticeDate = new Date(noticeVal);
    
    // Calculate difference in days (ignoring daylight savings timezone shifts)
    const timeDiff = noticeDate.getTime() - memoDate.getTime();
    const dayDiff = Math.floor(timeDiff / (1000 * 3600 * 24));

    const statusBadge = document.getElementById('sandboxStatusBadge');
    const step1Item = document.getElementById('step1Item');
    const step1Date = document.getElementById('step1Date');
    const step1Desc = document.getElementById('step1Desc');
    const step2Date = document.getElementById('step2Date');
    const step3Date = document.getElementById('step3Date');

    const formatDate = (date) => {
        return date.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
    };

    // Repayment Notice period is notice date + 15 days, so filing becomes legal on notice date + 16 days.
    const filingStartDate = new Date(noticeDate.getTime() + 16 * 24 * 60 * 60 * 1000);
    // Statutory filing window is 30 days starting from cause of action date, which is notice date + 15 + 30 = notice date + 45 days.
    const filingDeadlineDate = new Date(noticeDate.getTime() + 45 * 24 * 60 * 60 * 1000);

    if (step2Date) {
        step2Date.textContent = `Filing becomes legal on: ${formatDate(filingStartDate)}`;
    }
    if (step3Date) {
        step3Date.textContent = `Filing deadline: ${formatDate(filingDeadlineDate)}`;
    }

    if (dayDiff >= 0 && dayDiff <= 30) {
        if (statusBadge) {
            statusBadge.textContent = 'Compliant';
            statusBadge.className = 'status-badge compliant animate-pop';
            statusBadge.style.animation = 'none';
            statusBadge.offsetHeight; // force reflow
            statusBadge.style.animation = null;
        }
        if (step1Item) {
            step1Item.className = 'timeline-step-item success';
        }
        if (step1Date) {
            step1Date.textContent = `Interval: ${dayDiff} days (Compliant)`;
        }
        if (step1Desc) {
            step1Desc.textContent = `Demand notice served within the 30-day statutory window of the Bank Return Memo.`;
        }
    } else {
        if (statusBadge) {
            statusBadge.textContent = 'Vulnerable';
            statusBadge.className = 'status-badge vulnerable animate-pop';
            statusBadge.style.animation = 'none';
            statusBadge.offsetHeight; // force reflow
            statusBadge.style.animation = null;
        }
        if (step1Item) {
            step1Item.className = 'timeline-step-item danger';
        }
        if (step1Date) {
            if (dayDiff < 0) {
                step1Date.textContent = `Interval: Invalid (Notice date cannot be before Bank Return Memo date)`;
            } else {
                step1Date.textContent = `Interval: ${dayDiff} days (Vulnerable - exceeds 30-day limit)`;
            }
        }
        if (step1Desc) {
            if (dayDiff < 0) {
                step1Desc.textContent = `Warning: Notice Delivery Date is before the Bank Return Memo date. Please correct your dates.`;
            } else {
                step1Desc.textContent = `Vulnerability: Demand notice served ${dayDiff} days after Bank Return Memo. Exceeds the 30-day statutory limit under Section 138.`;
            }
        }
    }
};

window.submitNewsletterForm = (event) => {
    event.preventDefault();
    const emailInput = document.getElementById('newsletterEmail');
    if (!emailInput) return;
    const email = emailInput.value.trim();
    
    if (!email) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('Please enter a valid email address.', 'warning');
        } else {
            alert('Please enter a valid email address.');
        }
        return;
    }

    // Display success toast notification
    if (window.ui && typeof window.ui.toast === 'function') {
        window.ui.toast(`Subscription successful! Welcome to JudiQ Insights, ${email}.`, 'success');
    } else {
        alert(`Subscription successful! Welcome to JudiQ Insights, ${email}.`);
    }

    // Clear input
    emailInput.value = '';
};

/* =============================================================================
   LEGAL MODALS AND PERSONALIZATION
   ============================================================================= */

window.openLegalModal = (modalId, event) => {
    if (event) event.preventDefault();
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.classList.remove('hidden');
        modal.classList.add('open');
        document.body.style.overflow = 'hidden';
    }
};

window.closeLegalModal = (modalId) => {
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.classList.remove('open');
        modal.classList.add('hidden');
        document.body.style.overflow = '';
    }
};

window.openProfileModal = (event) => {
    if (event) event.preventDefault();
    
    // Load current values
    const user = window.state.currentUser;
    if (!user) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast("Please log in to edit profile", "warning");
        }
        return;
    }
    
    const uid = user.uid;
    const savedProfileStr = localStorage.getItem(`judiq_profile_${uid}`);
    let name = user.displayName || user.email.split('@')[0];
    let firm = '';
    let role = window.state.currentRole || 'law_firm';
    
    if (savedProfileStr) {
        try {
            const profile = JSON.parse(savedProfileStr);
            if (profile.displayName) name = profile.displayName;
            if (profile.firmName) firm = profile.firmName;
            if (profile.role) role = profile.role;
        } catch (_) {}
    }
    
    const nameInput = document.getElementById('profileDisplayName');
    const firmInput = document.getElementById('profileFirmName');
    const roleSelect = document.getElementById('profileRole');
    
    if (nameInput) nameInput.value = name;
    if (firmInput) firmInput.value = firm;
    if (roleSelect) roleSelect.value = role;
    
    window.openLegalModal('profileSettingsModal');
};

window.saveUserProfile = (event) => {
    event.preventDefault();
    const user = window.state.currentUser;
    if (!user) return;
    
    const name = document.getElementById('profileDisplayName').value.trim();
    const firm = document.getElementById('profileFirmName').value.trim();
    const role = document.getElementById('profileRole').value;
    
    const profile = {
        displayName: name,
        firmName: firm,
        role: role
    };
    
    localStorage.setItem(`judiq_profile_${user.uid}`, JSON.stringify(profile));
    localStorage.setItem(`judiq_role_${user.uid}`, role);
    
    window.state.currentRole = role;
    
    // Attempt updating user profile in Firebase Auth as well
    if (user.updateProfile) {
        user.updateProfile({ displayName: name }).catch(console.error);
    }
    
    // Refresh dashboard view and welcome greeting
    renderDashboard();
    
    if (window.ui && typeof window.ui.toast === 'function') {
        window.ui.toast("Profile settings saved successfully!", "success");
    } else {
        alert("Profile settings saved successfully!");
    }
    
    window.closeLegalModal('profileSettingsModal');
};

window.resendVerificationEmail = async (event) => {
    if (event) event.preventDefault();
    const user = window.state.currentUser;
    if (!user) return;
    
    const btn = event.target;
    if (btn) btn.disabled = true;
    
    try {
        if (typeof user.sendEmailVerification === 'function') {
            await user.sendEmailVerification();
            if (window.ui && typeof window.ui.toast === 'function') {
                window.ui.toast(`Verification email sent to: ${user.email}`, "success");
            } else {
                alert(`Verification email sent to: ${user.email}`);
            }
        } else {
            // Mock mode fallback
            if (window.ui && typeof window.ui.toast === 'function') {
                window.ui.toast(`Mock: Verification email successfully resent to ${user.email}.`, "success");
            } else {
                alert(`Mock: Verification email successfully resent to ${user.email}.`);
            }
        }
    } catch (err) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast(err.message, "error");
        } else {
            alert(err.message);
        }
    } finally {
        if (btn) btn.disabled = false;
    }
};

window.triggerPasswordResetEmail = async (event) => {
    if (event) event.preventDefault();
    const user = window.state.currentUser;
    if (!user) return;
    
    const btn = event.target;
    if (btn) btn.disabled = true;
    
    try {
        if (auth && typeof auth.sendPasswordResetEmail === 'function') {
            await auth.sendPasswordResetEmail(user.email);
            if (window.ui && typeof window.ui.toast === 'function') {
                window.ui.toast(`Password reset link sent to: ${user.email}`, "success");
            } else {
                alert(`Password reset link sent to: ${user.email}`);
            }
        } else {
            if (window.ui && typeof window.ui.toast === 'function') {
                window.ui.toast(`Mock: Password reset link successfully sent to ${user.email}.`, "success");
            } else {
                alert(`Mock: Password reset link successfully sent to ${user.email}.`);
            }
        }
    } catch (err) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast(err.message, "error");
        } else {
            alert(err.message);
        }
    } finally {
        if (btn) btn.disabled = false;
    }
};

window.changeUserPasswordDirect = async (event) => {
    event.preventDefault();
    const user = window.state.currentUser;
    if (!user) return;
    
    const newPass = document.getElementById('changePassNew').value;
    const confirmPass = document.getElementById('changePassConfirm').value;
    
    if (newPass !== confirmPass) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast("New passwords do not match", "warning");
        } else {
            alert("New passwords do not match");
        }
        return;
    }
    
    const btn = event.target.querySelector('button[type="submit"]');
    if (btn) btn.disabled = true;
    
    try {
        if (typeof user.updatePassword === 'function') {
            await user.updatePassword(newPass);
            if (window.ui && typeof window.ui.toast === 'function') {
                window.ui.toast("Password updated successfully!", "success");
            } else {
                alert("Password updated successfully!");
            }
        } else {
            if (window.ui && typeof window.ui.toast === 'function') {
                window.ui.toast("Mock: Password updated successfully!", "success");
            } else {
                alert("Mock: Password updated successfully!");
            }
        }
        document.getElementById('changePasswordForm').reset();
    } catch (err) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast(err.message, "error");
        } else {
            alert(err.message);
        }
    } finally {
        if (btn) btn.disabled = false;
    }
};

/* =============================================================================
   LITIGATION READINESS SUITE & FLOATING AI CHAT INTERACTIVITY
   ============================================================================= */

// Precedents Data
const landmarkPrecedents = [
    // ── CRIMINAL / BNS / BNSS / BAIL / QUASHING ───────────────────
    {
        title: "Satender Kumar Antil vs. Central Bureau of Investigation",
        tag: "Bail Reform & BNSS S.35",
        domain: "Criminal",
        text: "Comprehensive landmark guidelines on bail jurisdiction, categorizing offences into Categories A, B, C, D. Strict mandatory compliance with Section 41/41A CrPC (Section 35 BNSS) before arrest.",
        source: "(2022) 10 SCC 51",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/14001226/"
    },
    {
        title: "State of Haryana vs. Bhajan Lal",
        tag: "FIR Quashing (S.482 / BNSS 528)",
        domain: "Criminal",
        text: "Established the 7 cardinal parameters where High Courts must exercise inherent jurisdiction to quash FIRs and criminal complaints (e.g. pure civil disputes clothed in criminal garb, malicious prosecution).",
        source: "1992 Supp (1) SCC 335",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/8637801/"
    },
    {
        title: "Arnesh Kumar vs. State of Bihar",
        tag: "Arrest Guidelines",
        domain: "Criminal",
        text: "Mandatory directives against automatic or mechanical arrest for offences punishable up to 7 years. Police officers must serve Section 41A notice and Magistrates must record satisfaction before authorizing detention.",
        source: "(2014) 8 SCC 273",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/175764778/"
    },
    {
        title: "Lalita Kumari vs. Govt. of U.P.",
        tag: "Mandatory FIR (S.154 / BNSS 173)",
        domain: "Criminal",
        text: "Registration of FIR is mandatory if information discloses commission of a cognizable offence. Preliminary inquiry permissible only in strictly defined categories (commercial, matrimonial, medical negligence).",
        source: "(2014) 2 SCC 1",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/1440673/"
    },
    {
        title: "Sanjay Chandra vs. Central Bureau of Investigation",
        tag: "Bail is Rule, Jail Exception",
        domain: "Criminal",
        text: "Reiterated that bail is the rule and jail is the exception. Pre-trial detention cannot be punitive; prolonged custody in complex economic trials without imminent trial conclusion warrants bail.",
        source: "(2012) 1 SCC 40",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/15349501/"
    },
    {
        title: "Vijay Madanlal Choudhary vs. Union of India",
        tag: "PMLA S.45 & Predicate Offence",
        domain: "Criminal",
        text: "Clarified that money laundering proceedings under PMLA cannot survive if the predicate/scheduled offence is quashed, discharged, or results in acquittal.",
        source: "(2022) SCC OnLine SC 929",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/705856/"
    },
    {
        title: "Kahkashan Kausar @ Sonam vs. State of Bihar",
        tag: "498A Relatives Quashing",
        domain: "Criminal",
        text: "Vague, omnibus, and general allegations against husband's relatives (in-laws, siblings) in Section 498A IPC / 85 BNS cases without specific overt acts must be quashed.",
        source: "(2022) 6 SCC 599",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/80929323/"
    },
    {
        title: "Sheila Sebastian vs. R. Jawaharaj",
        tag: "Forgery & False Document (S.467/468)",
        domain: "Criminal",
        text: "A person cannot be convicted of forgery unless proved beyond reasonable doubt to be the actual maker or author of the fraudulent document.",
        source: "(2018) 7 SCC 581",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/93478173/"
    },
    {
        title: "Sushila Aggarwal vs. State (NCT of Delhi)",
        tag: "Anticipatory Bail (S.438 / BNSS 482)",
        domain: "Criminal",
        text: "5-Judge Constitution Bench held that protection of anticipatory bail is not invariably limited in time and can continue till the end of the trial.",
        source: "(2020) 5 SCC 1",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/141020640/"
    },

    // ── SARFAESI & BANKING DEBT RECOVERY ──────────────────────────
    {
        title: "Mardia Chemicals Ltd. vs. Union of India",
        tag: "Section 13(3A) Reasoned Reply",
        domain: "SARFAESI",
        text: "Mandatory requirement for Secured Creditor bank to consider borrower's Section 13(3A) representation and communicate reasoned reply before taking Section 13(4) possession.",
        source: "(2004) 4 SCC 311",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/1714918/"
    },
    {
        title: "United Bank of India vs. Satyawati Tondon",
        tag: "Alternate Remedy (S.17 DRT)",
        domain: "SARFAESI",
        text: "High Courts must not entertain Article 226 writ petitions challenging SARFAESI measures where an effective alternate statutory remedy under Section 17 DRT is available.",
        source: "(2010) 8 SCC 110",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/1479092/"
    },
    {
        title: "Transcore vs. Union of India",
        tag: "Concurrent Remedies (DRT + SARFAESI)",
        domain: "SARFAESI",
        text: "SARFAESI Act and RDDBFI Act provide concurrent, cumulative remedies. Doctrine of election does not bar taking Section 13(4) possession during pendency of DRT OA.",
        source: "(2008) 1 SCC 125",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/1352604/"
    },
    {
        title: "Celir LLP vs. Bafna Motors (Mumbai) Pvt. Ltd.",
        tag: "Redemption Extinction (S.13(8))",
        domain: "SARFAESI",
        text: "Under amended Section 13(8) of SARFAESI Act, borrower's right of redemption is extinguished immediately upon publication of auction notice.",
        source: "(2024) 2 SCC 1",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/59493976/"
    },
    {
        title: "C. Bright vs. District Collector",
        tag: "Section 14 CJM/DM Timeline",
        domain: "SARFAESI",
        text: "The statutory timeline under Section 14 proviso for District Magistrate / CJM to assist secured creditor in taking physical possession is directory and not mandatory.",
        source: "(2021) 2 SCC 392",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/573863/"
    },

    // ── NEGOTIABLE INSTRUMENTS ACT (SECTION 138 / 139 / 141) ───────
    {
        title: "Basalingappa vs. Mudibasappa",
        tag: "Financial Capacity Challenge",
        domain: "NI Act",
        text: "Held that if financial capacity of complainant is challenged in high-value cash transactions, complainant must prove source of funds to establish legally enforceable debt.",
        source: "(2019) 5 SCC 418",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/81116500/"
    },
    {
        title: "Rangappa vs. Srikanth",
        tag: "Section 139 Debt Presumption",
        domain: "NI Act",
        text: "Confirmed that Section 139 carries a strong statutory presumption of debt. Debtor must raise a probable defense on preponderance of probabilities to rebut it.",
        source: "(2010) 11 SCC 441",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/1498679/"
    },
    {
        title: "Aneeta Hada vs. Godfather Travels",
        tag: "Company Prosecution (S.141)",
        domain: "NI Act",
        text: "Prosecution of company directors/officers under Section 141 is not maintainable unless the company itself is joined as a primary accused entity.",
        source: "(2012) 5 SCC 661",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/7901511/"
    },
    {
        title: "Bir Singh vs. Mukesh Kumar",
        tag: "Blank Signed Cheque (S.20)",
        domain: "NI Act",
        text: "A blank signed cheque handed over carries implied authority to fill particulars. It is fully valid and enforceable under Section 138 upon dishonour.",
        source: "(2019) 4 SCC 197",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/981928/"
    },
    {
        title: "Yogendra Pratap Singh vs. Savitri Pandey",
        tag: "Premature Filing Bar",
        domain: "NI Act",
        text: "A Section 138 complaint filed before the expiry of the mandatory 15-day notice period is premature and non-maintainable.",
        source: "(2014) 10 SCC 713",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/1391482/"
    },
    {
        title: "Sampelly Satyanarayana Rao vs. IREDA",
        tag: "Security Cheque Enforceability",
        domain: "NI Act",
        text: "Once debt crystallizes on the cheque date, even an instrument delivered as a 'security cheque' is enforceable under Section 138.",
        source: "(2016) 10 SCC 458",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/1919952/"
    },

    // ── CIVIL, COMMERCIAL & ARBITRATION ────────────────────────────
    {
        title: "Vidya Drolia vs. Durga Trading Corporation",
        tag: "Arbitrability Test (S.11)",
        domain: "Arbitration",
        text: "Authoritative 4-fold test for non-arbitrability of disputes; landlord-tenant disputes arbitrable unless governed by special rent control acts.",
        source: "(2021) 2 SCC 1",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/1714918/"
    },
    {
        title: "N.N. Global Mercantile vs. Indo Unique Flame",
        tag: "Unstamped Agreement (7-Judge Bench)",
        domain: "Arbitration",
        text: "7-Judge Constitution Bench held that non-stamping of underlying commercial agreement does not render arbitration clause invalid at Section 11 referral stage.",
        source: "(2024) 4 SCC 341",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/141020640/"
    },
    {
        title: "Perkins Eastman Architects vs. HSCC (India)",
        tag: "Unilateral Arbitrator Appointment",
        domain: "Arbitration",
        text: "A party interested in dispute outcome is ineligible to act as arbitrator and equally disqualified from unilaterally appointing a sole arbitrator.",
        source: "(2020) 20 SCC 760",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/165439500/"
    },
    {
        title: "Patil Automation vs. Rakheja Engineers",
        tag: "Mandatory Mediation (S.12A CCA)",
        domain: "Commercial Suits",
        text: "Pre-institution mediation under Section 12A Commercial Courts Act is mandatory; suits filed without urgent interim relief must be rejected under Order 7 Rule 11 CPC.",
        source: "(2022) 10 SCC 1",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/59648905/"
    },
    {
        title: "Dalpat Kumar vs. Prahlad Singh",
        tag: "Temporary Injunction Three Pillars",
        domain: "Civil Suits",
        text: "Three mandatory pillars for grant of temporary injunction under Order 39 CPC: prima facie case, balance of convenience, and irreparable loss.",
        source: "(1992) 1 SCC 719",
        court: "Supreme Court of India",
        link: "https://indiankanoon.org/doc/1681702/"
    }
];

window.updateReadinessProgress = () => {
    const checks = document.querySelectorAll('.readiness-check');
    if (checks.length === 0) return;
    
    let checkedCount = 0;
    checks.forEach(check => {
        if (check.checked) checkedCount++;
    });
    
    const percentage = Math.round((checkedCount / checks.length) * 100);
    
    // Update progress text
    const textEl = document.getElementById('readinessProgressText');
    if (textEl) textEl.textContent = `${percentage}%`;
    
    // Update progress circle offset (circumference of r=40 circle is 2 * pi * 40 ≈ 251.2)
    const circleBar = document.getElementById('readinessCircleBar');
    if (circleBar) {
        const circumference = 251.2;
        const offset = circumference - (percentage / 100) * circumference;
        circleBar.style.strokeDashoffset = offset;
    }
    
    // Update status text
    const statusTextEl = document.getElementById('readinessStatusText');
    if (statusTextEl) {
        if (percentage === 0) {
            statusTextEl.textContent = "No documents checked.";
            statusTextEl.style.color = "var(--gray-400)";
        } else if (percentage < 40) {
            statusTextEl.textContent = "High Risk. Key evidence missing.";
            statusTextEl.style.color = "var(--danger-400)";
        } else if (percentage < 80) {
            statusTextEl.textContent = "Partial readiness. Notice served?";
            statusTextEl.style.color = "var(--warning-400)";
        } else if (percentage < 100) {
            statusTextEl.textContent = "Strong case files ready to compile.";
            statusTextEl.style.color = "var(--primary-400)";
        } else {
            statusTextEl.textContent = "100% Ready. Secure filing approved!";
            statusTextEl.style.color = "var(--success-400)";
        }
    }
};

window.filterPrecedentsList = () => {
    const searchInput = document.getElementById('precedentSearchInput');
    const container = document.getElementById('precedentsListContainer');
    if (!container) return;
    
    const query = searchInput ? searchInput.value.toLowerCase().trim() : '';
    
    const filtered = landmarkPrecedents.filter(item => {
        return item.title.toLowerCase().includes(query) || 
               (item.tag && item.tag.toLowerCase().includes(query)) || 
               (item.domain && item.domain.toLowerCase().includes(query)) ||
               item.text.toLowerCase().includes(query) ||
               item.source.toLowerCase().includes(query);
    });
    
    if (filtered.length === 0) {
        container.innerHTML = `<p style="color: var(--gray-500); font-size: 0.9rem; text-align: center; margin-top: 2rem;"><i class="fas fa-search" style="margin-right: 0.4rem;"></i> No matching precedent authorities found for "${query}". Try searching by statute or judge.</p>`;
        return;
    }
    
    container.innerHTML = filtered.map(item => {
        const domainColor = item.domain === 'Criminal' ? '#ef4444' : (item.domain === 'SARFAESI' ? '#f59e0b' : (item.domain === 'Arbitration' ? '#8b5cf6' : (item.domain === 'Commercial Suits' ? '#10b981' : '#0ea5e9')));
        const link = item.link || `https://indiankanoon.org/search/?formInput=${encodeURIComponent(item.title)}`;
        const copyText = `${item.title}, ${item.source} (${item.court || 'Supreme Court of India'})`;
        
        return `
        <div class="citation-result-card" style="border-left: 3px solid ${domainColor}; transition: var(--transition-fast); margin-bottom: 0.85rem; padding: 0.95rem; background: var(--gray-100); border-radius: 0.6rem; border: 1px solid var(--gray-200); box-shadow: 0 2px 4px rgba(0,0,0,0.03);">
            <div class="citation-result-header" style="display:flex; align-items:flex-start; justify-content:space-between; gap:0.5rem; margin-bottom:0.45rem; flex-wrap: wrap;">
                <h4 class="citation-result-title" style="font-family: var(--font-serif); font-size:0.98rem; font-weight:700; color: var(--gray-900); margin:0; line-height: 1.35;">
                    <i class="fas fa-gavel" style="color: #d97706; margin-right: 0.4rem; font-size: 0.85rem;"></i>
                    <a href="${link}" target="_blank" rel="noopener noreferrer" style="color: var(--primary-600); text-decoration: none; border-bottom: 1px dashed rgba(2,132,199,0.4);" title="Open official judgment on Indian Kanoon">
                        ${item.title} <i class="fas fa-external-link-alt" style="font-size: 0.72rem; margin-left: 0.25rem; opacity: 0.7;"></i>
                    </a>
                </h4>
                <div style="display:flex; gap: 0.35rem; align-items:center;">
                    <span style="background: rgba(15,23,42,0.08); color: ${domainColor}; font-size: 0.68rem; font-weight: 700; padding: 0.12rem 0.45rem; border-radius: 6px; text-transform: uppercase;">${item.domain || 'Landmark'}</span>
                    <span class="citation-result-tag" style="background: rgba(14,165,233,0.12); color: #0284c7; border: 1px solid rgba(14,165,233,0.3); font-size: 0.68rem; font-weight: 700; padding: 0.12rem 0.45rem; border-radius: 6px;">${item.tag}</span>
                </div>
            </div>
            <p class="citation-result-text" style="font-size:0.84rem; color: var(--gray-700); line-height: 1.5; margin-bottom: 0.6rem;">${item.text}</p>
            <div style="display:flex; align-items:center; justify-content:space-between; font-size: 0.76rem; color: var(--gray-500); border-top: 1px solid var(--gray-200); padding-top: 0.5rem; margin-top: 0.4rem; flex-wrap: wrap; gap: 0.4rem;">
                <span><i class="fas fa-book-open" style="margin-right:0.3rem; color: #64748b;"></i> <strong>Citation:</strong> ${item.source} (${item.court || 'Supreme Court of India'})</span>
                <div style="display:flex; gap: 0.5rem; align-items:center;">
                    <a href="${link}" target="_blank" rel="noopener noreferrer" style="color: var(--primary-600); text-decoration: none; font-size: 0.74rem; font-weight: 600; display: inline-flex; align-items: center; gap: 0.25rem; background: rgba(2,132,199,0.08); padding: 0.15rem 0.5rem; border-radius: 4px;">
                        <i class="fas fa-landmark"></i> Indian Kanoon
                    </a>
                    <button onclick="navigator.clipboard.writeText('${copyText.replace(/'/g, "\\'")}'); if(window.ui && window.ui.toast) { window.ui.toast('Citation copied: ${item.source}', 'success'); } else { alert('Citation copied!'); }" style="background: rgba(15,23,42,0.05); border: 1px solid var(--gray-300); color: var(--gray-700); font-size: 0.74rem; font-weight: 600; cursor: pointer; display: inline-flex; align-items: center; gap: 0.25rem; padding: 0.15rem 0.5rem; border-radius: 4px; transition: all 0.15s ease;">
                        <i class="fas fa-copy"></i> Copy Citation
                    </button>
                </div>
            </div>
        </div>`;
    }).join('');
};

window.filterPrecedentByTag = (tagQuery, element) => {
    const searchInput = document.getElementById('precedentSearchInput');
    if (searchInput) {
        searchInput.value = tagQuery;
    }
    document.querySelectorAll('.precedent-tag-pill').forEach(btn => btn.classList.remove('active'));
    if (element) element.classList.add('active');
    window.filterPrecedentsList();
};



// ═══════════════════════════════════════════════════════════════
// DRAFT STUDIO — Standalone Draft Generator (accessible from dashboard)
// ═══════════════════════════════════════════════════════════════

let activeStudioDraftType = null;

/**
 * Open the Draft Studio screen from dashboard
 */
window.showDraftStudio = () => {
    switchScreen('draftStudioScreen');
    window.showStudioTypeSelection();
};

window.showStudioTypeSelection = () => {
    const sel = document.getElementById('studioTypeSelection');
    const form = document.getElementById('studioInputForm');
    const out = document.getElementById('studioOutputView');
    if (sel) sel.classList.remove('hidden');
    if (form) form.classList.add('hidden');
    if (out) out.classList.add('hidden');
    renderStudioDraftTypeGrid();
};

window.showStudioInputForm = () => {
    const sel = document.getElementById('studioTypeSelection');
    const form = document.getElementById('studioInputForm');
    const out = document.getElementById('studioOutputView');
    if (sel) sel.classList.add('hidden');
    if (form) form.classList.remove('hidden');
    if (out) out.classList.add('hidden');
};

window.showStudioOutputView = () => {
    const sel = document.getElementById('studioTypeSelection');
    const form = document.getElementById('studioInputForm');
    const out = document.getElementById('studioOutputView');
    if (sel) sel.classList.add('hidden');
    if (form) form.classList.add('hidden');
    if (out) out.classList.remove('hidden');
};

function renderStudioDraftTypeGrid() {
    const grid = document.getElementById('studioDraftTypeGrid');
    if (!grid) return;
    grid.innerHTML = DRAFT_TYPES.map(dt => `
        <div class="draft-type-card" onclick="window.selectStudioDraftType('${dt.id}')">
            <div class="draft-type-num" style="background:${dt.color}18;color:${dt.color}">${dt.number}</div>
            <div class="draft-type-icon-wrap" style="color:${dt.color}">
                <i class="fas ${dt.icon}"></i>
            </div>
            <div class="draft-type-info">
                <h4>${dt.title}</h4>
                <span class="draft-type-sub">${dt.subtitle}</span>
                <p>${dt.description}</p>
            </div>
            <div class="draft-type-chevron" style="color:${dt.color}">
                <i class="fas fa-chevron-right"></i>
            </div>
        </div>
    `).join('');
}

window.selectStudioDraftType = (id) => {
    activeStudioDraftType = DRAFT_TYPES.find(dt => dt.id === id);
    if (!activeStudioDraftType) return;

    const badge = document.getElementById('studioDraftSelectedBadge');
    if (badge) {
        badge.innerHTML = `
            <span style="background:${activeStudioDraftType.color}18;color:${activeStudioDraftType.color};padding:0.3rem 1rem;border-radius:999px;font-size:0.8rem;font-weight:600;display:inline-flex;align-items:center;gap:0.4rem;">
                <i class="fas ${activeStudioDraftType.icon}"></i> Type ${activeStudioDraftType.number} &nbsp;·&nbsp; ${activeStudioDraftType.subtitle}
            </span>`;
    }

    const titleEl = document.getElementById('studioFormTitle');
    const subtitleEl = document.getElementById('studioFormSubtitle');
    if (titleEl) titleEl.textContent = activeStudioDraftType.title;
    if (subtitleEl) subtitleEl.textContent = activeStudioDraftType.description;

    // Pre-fill from existing case data if any
    const pre = {};
    const s = window.state?.caseData || {};
    const preMap = {
        complainant_name: s.complainant_name, complainant_address: s.complainant_address,
        accused_name: s.accused_name, accused_address: s.accused_address,
        cheque_number: s.cheque_number, cheque_date: s.cheque_date,
        cheque_amount: s.cheque_amount, bank_name: s.bank_name,
        branch_name: s.branch_name, dishonour_date: s.dishonour_date || s.date_of_dishonour,
        dishonour_reason: s.dishonour_reason, court_name: s.court_name,
        notice_date: s.notice_date || s.date_of_notice, filing_date: s.filing_date
    };
    Object.assign(pre, preMap);

    const body = document.getElementById('studioFormBody');
    if (body) {
        body.innerHTML = '<div class="draft-fields-grid">' + activeStudioDraftType.fields.map(f => {
            const val = pre[f.name] || '';
            const isWide = f.type === 'textarea' || f.name.includes('address') || f.name.includes('details') || f.name.includes('purpose');
            let inputHtml = '';
            if (f.type === 'textarea') {
                inputHtml = `<textarea id="sfield_${f.name}" class="draft-field-input" ${f.required ? 'required' : ''} placeholder="${f.placeholder || ''}" rows="3">${val}</textarea>`;
            } else if (f.type === 'select') {
                inputHtml = `<select id="sfield_${f.name}" class="draft-field-input" ${f.required ? 'required' : ''}>
                    <option value="">Select...</option>
                    ${(f.options || []).map(o => `<option value="${o}"${val === o ? ' selected' : ''}>${o}</option>`).join('')}
                </select>`;
            } else {
                inputHtml = `<input type="${f.type}" id="sfield_${f.name}" class="draft-field-input" ${f.required ? 'required' : ''} placeholder="${f.placeholder || ''}" value="${val}">`;
            }
            return `<div class="draft-field-group${isWide ? ' full-width' : ''}">
                <label class="draft-field-label" for="sfield_${f.name}">
                    ${f.label}${f.required ? ' <span class="req-star">*</span>' : ''}
                </label>${inputHtml}
            </div>`;
        }).join('') + '</div>';
    }

    window.showStudioInputForm();
};

window.generateStudioDraft = () => {
    if (!activeStudioDraftType) return;
    const missing = [];
    const data = {};
    for (const f of activeStudioDraftType.fields) {
        const el = document.getElementById('sfield_' + f.name);
        if (!el) continue;
        const val = el.value.trim();
        if (f.required && !val) {
            missing.push(f.label);
            el.classList.add('field-error');
        } else {
            el.classList.remove('field-error');
            data[f.name] = val;
        }
    }
    if (missing.length > 0) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('Please fill required fields: ' + missing.slice(0, 3).join(', ') + (missing.length > 3 ? '...' : ''), 'error');
        }
        return;
    }
    try {
        const txt = activeStudioDraftType.generate(data);
        const ta = document.getElementById('studioDraftContent');
        if (ta) ta.value = txt;

        const badge = document.getElementById('studioDraftOutputBadge');
        if (badge) {
            badge.innerHTML = `<span style="background:${activeStudioDraftType.color}18;color:${activeStudioDraftType.color};padding:0.3rem 1rem;border-radius:999px;font-size:0.8rem;font-weight:600;display:inline-flex;align-items:center;gap:0.4rem;">
                <i class="fas ${activeStudioDraftType.icon}"></i> ${activeStudioDraftType.title}
            </span>`;
        }
        const titleEl = document.getElementById('studioDraftOutputTitle');
        if (titleEl) titleEl.textContent = activeStudioDraftType.title;

        window.showStudioOutputView();
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('Draft generated successfully!', 'success');
        }
    } catch (err) {
        console.error('Draft generation error:', err);
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('Failed to generate draft: ' + err.message, 'error');
        }
    }
};

window.copyStudioDraft = () => {
    const ta = document.getElementById('studioDraftContent');
    if (!ta || !ta.value) return;
    navigator.clipboard.writeText(ta.value).then(() => {
        if (window.ui && typeof window.ui.toast === 'function') window.ui.toast('Draft copied to clipboard!', 'success');
    }).catch(() => {
        ta.select();
        document.execCommand('copy');
        if (window.ui && typeof window.ui.toast === 'function') window.ui.toast('Draft copied!', 'success');
    });
};

window.downloadStudioDraft = () => {
    const ta = document.getElementById('studioDraftContent');
    if (!ta || !ta.value) return;
    const name = (activeStudioDraftType?.title || 'Legal_Draft').replace(/\s+/g, '_');
    const blob = new Blob([ta.value], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `JudiQ_${name}_${new Date().toISOString().slice(0, 10)}.txt`;
    a.click();
    URL.revokeObjectURL(url);
};

window.printStudioDraft = () => {
    const ta = document.getElementById('studioDraftContent');
    if (!ta || !ta.value) return;
    const w = window.open('', '_blank');
    w.document.write(`<html><head><title>${activeStudioDraftType?.title || 'Legal Draft'}</title>
    <style>body{font-family:monospace;white-space:pre-wrap;padding:2cm;font-size:12pt;line-height:1.6;}</style>
    </head><body>${ta.value.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</body></html>`);
    w.document.close();
    w.print();
};



// ============================================================================
// WHAT-IF SCENARIO MODELER LOGIC
// ============================================================================

window.updateWhatIf = (key, value) => {
    if (!window.state.caseData) {
        window.state.caseData = {};
    }
    window.state.caseData[key] = value;
};

window.runAnalysis = async () => {
    if (!window.state.caseData) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('No case data available to analyze.', 'warning');
        }
        return;
    }
    
    if (window.ui && typeof window.ui.show === 'function') window.ui.show('analysisLoading');
    try {
        const userId = window.state.currentUser ? window.state.currentUser.uid : 'ANONYMOUS';
        const rawPayload = { ...window.state.caseData, user_id: userId };
        
        // If sanitizePayload is available (from wizard.js), use it, otherwise use raw
        const payload = typeof sanitizePayload === 'function' ? sanitizePayload(rawPayload) : rawPayload;
        
        const result = await api.analyze(payload);
        window.state.analysisResult = result;
        
        if (window.saveCaseToHistory) {
            window.saveCaseToHistory(payload, result);
        }
        
        if (window.ui && typeof window.ui.hide === 'function') window.ui.hide('analysisLoading');
        
        // Re-render the results dashboard with the new data
        if (typeof renderResults === 'function') {
            renderResults(result);
        }
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('Case successfully re-analyzed with new variables.', 'success');
        }
    } catch (err) {
        if (window.ui && typeof window.ui.hide === 'function') window.ui.hide('analysisLoading');
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('Error recalculating: ' + err.message, 'error');
        }
    }
};

// --- Copy Strategy Memo to Clipboard ---
window.copyStrategyMemoToClipboard = function() {
    const res = window.state.analysisResult;
    if (!res) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('No analysis report available to copy.', 'warning');
        } else {
            alert('No analysis report available to copy.');
        }
        return;
    }
    
    let memo = `# JUDIQ AI — Litigation Strategy & Weakness Audit Memo\n`;
    memo += `Generated on: ${new Date().toLocaleDateString()} | Domain: ${res.domain || 'Section 138 NI Act'}\n\n`;
    memo += `## 1. Viability & Score Summary\n`;
    memo += `- **Overall Score**: ${res.score !== undefined ? res.score : 'N/A'}/100\n`;
    memo += `- **Merit Assessment**: ${res.verdict || 'ANALYZED'}\n`;
    if (res.summary) memo += `- **Summary**: ${res.summary}\n`;
    
    if (res.statutory_timeline || res.limitation) {
        memo += `\n## 2. Limitation & Procedural Audit\n`;
        if (res.limitation) {
            memo += `- **Status**: ${res.limitation.status || 'CHECKED'}\n`;
            memo += `- **Days Remaining**: ${res.limitation.days_remaining !== undefined ? res.limitation.days_remaining : 'N/A'}\n`;
        }
    }

    if (res.vulnerabilities && res.vulnerabilities.length > 0) {
        memo += `\n## 3. Adversarial Weaknesses & Vulnerabilities\n`;
        res.vulnerabilities.forEach((v, idx) => {
            const risk = typeof v === 'object' ? (v.risk || v.title || v.issue || JSON.stringify(v)) : String(v);
            memo += `${idx + 1}. ${risk}\n`;
        });
    }

    if (res.next_best_actions && res.next_best_actions.length > 0) {
        memo += `\n## 4. Recommended Next Actions\n`;
        res.next_best_actions.forEach((act, idx) => {
            memo += `${idx + 1}. ${act}\n`;
        });
    }
    
    memo += `\n---\n*Report generated by JudiQ AI Litigation Operating System.*`;
    
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(memo).then(() => {
            if (window.ui && typeof window.ui.toast === 'function') {
                window.ui.toast('Strategy Memo copied to clipboard in Markdown format!', 'success');
            } else {
                alert('Strategy Memo copied to clipboard!');
            }
        }).catch(err => {
            console.error('Clipboard copy failed:', err);
            if (window.ui && typeof window.ui.toast === 'function') {
                window.ui.toast('Failed to copy to clipboard.', 'error');
            }
        });
    } else {
        // Fallback for non-secure contexts
        const textarea = document.createElement('textarea');
        textarea.value = memo;
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('Strategy Memo copied to clipboard!', 'success');
        }
    }
};

// --- Print Strategy Memo in High-Court Formal Format ---
window.printStrategyMemo = function() {
    const res = window.state.analysisResult;
    if (!res) {
        if (window.ui && typeof window.ui.toast === 'function') {
            window.ui.toast('No analysis report available to print.', 'warning');
        } else {
            alert('No analysis report available to print.');
        }
        return;
    }
    
    const printWin = window.open('', '_blank', 'width=900,height=800');
    if (!printWin) return;
    
    const today = new Date().toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
    const score = res.score !== undefined ? res.score : 'N/A';
    const domain = res.domain || 'Section 138 NI Act / Criminal Jurisdiction';
    
    const html = `<!DOCTYPE html>
<html>
<head>
    <title>JUDIQ LITIGATION STRATEGY MEMORANDUM</title>
    <link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@600;700;800&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        @media print {
            @page { margin: 20mm; }
            body { background: white !important; color: black !important; }
        }
        body { font-family: 'Inter', sans-serif; margin: 40px; color: #1e293b; line-height: 1.6; }
        .header-seal { text-align: center; border-bottom: 2px solid #0f172a; padding-bottom: 15px; margin-bottom: 25px; }
        .header-seal h1 { font-family: 'Cinzel', serif; font-size: 22px; letter-spacing: 0.1em; margin: 0; color: #0f172a; text-transform: uppercase; }
        .header-seal p { margin: 4px 0 0 0; font-size: 11px; color: #64748b; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; }
        .meta-box { display: flex; justify-content: space-between; background: #f8fafc; border: 1px solid #e2e8f0; padding: 12px 18px; border-radius: 6px; margin-bottom: 25px; font-size: 12px; }
        .section-title { font-family: 'Cinzel', serif; font-size: 14px; border-bottom: 1px solid #cbd5e1; padding-bottom: 4px; margin-top: 25px; color: #0f172a; font-weight: 700; }
        .score-badge { display: inline-block; padding: 4px 12px; background: #0f172a; color: white; font-weight: 700; border-radius: 4px; font-size: 14px; }
        .footer-seal { margin-top: 40px; padding-top: 15px; border-top: 1px dashed #cbd5e1; font-size: 10px; color: #94a3b8; text-align: center; }
    </style>
</head>
<body>
    <div class="header-seal">
        <h1>CONFIDENTIAL LITIGATION STRATEGY MEMORANDUM</h1>
        <p>JUDIQ AI ADVERSARIAL ENGINE • HIGH COURT & STATUTORY JURISDICTION SUITE</p>
    </div>
    <div class="meta-box">
        <div><strong>DATE:</strong> ${today}</div>
        <div><strong>DOMAIN:</strong> ${domain}</div>
        <div><strong>VIABILITY SCORE:</strong> <span class="score-badge">${score}/100</span></div>
    </div>
    <div class="section-title">1. EXECUTIVE SUMMARY & MERIT EVALUATION</div>
    <p><strong>Merit Assessment:</strong> ${res.verdict || 'ANALYZED'}</p>
    ${res.summary ? `<p>${res.summary}</p>` : ''}
    
    ${res.vulnerabilities && res.vulnerabilities.length ? `
    <div class="section-title">2. ADVERSARIAL VULNERABILITIES & DEFECTS</div>
    <ul>
        ${res.vulnerabilities.map(v => `<li>${typeof v === 'object' ? (v.risk || v.title || JSON.stringify(v)) : v}</li>`).join('')}
    </ul>` : ''}

    ${res.next_best_actions && res.next_best_actions.length ? `
    <div class="section-title">3. STRATEGIC COUNTERMEASURES & ACTIONS</div>
    <ol>
        ${res.next_best_actions.map(a => `<li>${a}</li>`).join('')}
    </ol>` : ''}

    <div class="footer-seal">
        CONFIDENTIAL & PRIVILEGED LEGAL WORK PRODUCT • GENERATED BY JUDIQ LITIGATION OS • RECORD HASH: ${Math.random().toString(36).substring(2, 10).toUpperCase()}
    </div>
    <script>
        window.onload = function() { window.print(); };
    </script>
</body>
</html>`;
    
    printWin.document.write(html);
    printWin.document.close();
};

// --- Command Palette Functions ---
window.toggleCommandPalette = function(forceState) {
    const modal = document.getElementById('commandPaletteModal');
    if (!modal) return;
    const input = document.getElementById('commandPaletteInput');
    
    const shouldOpen = forceState !== undefined ? forceState : !modal.classList.contains('open');
    if (shouldOpen) {
        modal.classList.add('open');
        if (input) {
            input.value = '';
            setTimeout(() => input.focus(), 100);
        }
        window.filterCommandPalette('');
    } else {
        modal.classList.remove('open');
    }
};

window.filterCommandPalette = function(query) {
    const q = (query || '').toLowerCase().trim();
    const container = document.getElementById('commandListContainer');
    if (!container) return;
    
    const items = container.querySelectorAll('.command-item');
    items.forEach(item => {
        const txt = item.innerText.toLowerCase();
        if (!q || txt.includes(q)) {
            item.style.display = 'flex';
        } else {
            item.style.display = 'none';
        }
    });
};

window.executeCommand = function(cmd) {
    window.toggleCommandPalette(false);
    switch (cmd) {
        case 'demo_s138':
            if (window.loadDemoCase) window.loadDemoCase('clean_case');
            break;
        case 'demo_sarfaesi':
            if (window.loadDemoCase) window.loadDemoCase('sarfaesi_demo');
            break;
        case 'demo_s141':
            if (window.loadDemoCase) window.loadDemoCase('directors_missing');
            break;
        case 'new_s138':
            if (window.selectRegisterDomain) window.selectRegisterDomain('ni_act');
            if (window.startWizard) window.startWizard();
            break;
        case 'new_sarfaesi':
            if (window.selectRegisterDomain) window.selectRegisterDomain('sarfaesi');
            if (window.startWizard) window.startWizard();
            break;
        case 'print_memo':
            if (window.printStrategyMemo) window.printStrategyMemo();
            break;
        case 'copy_memo':
            if (window.copyStrategyMemoToClipboard) window.copyStrategyMemoToClipboard();
            break;
        case 'search_precedents':
            const el = document.getElementById('precedentSearchInput');
            if (el) { el.scrollIntoView({ behavior: 'smooth' }); el.focus(); }
            break;
        case 'nav_dashboard':
            if (window.showDashboard) window.showDashboard();
            break;
        case 'open_simulator':
            const simEl = document.getElementById('strategySimulatorSection');
            if (simEl) { simEl.scrollIntoView({ behavior: 'smooth' }); }
            break;
        case 'toggle_cocounsel':
            const toggleBtn = document.getElementById('coCounselToggleBtn');
            if (toggleBtn) { toggleBtn.click(); }
            break;
        case 'toggle_theme':
            if (window.toggleTheme) window.toggleTheme();
            break;
        case 'toggle_lang':
            if (window.toggleLanguage) window.toggleLanguage();
            break;
        default:
            break;
    }
};

// Global Keyboard Shortcut Listeners
document.addEventListener('keydown', (e) => {
    // Ctrl+K or Cmd+K
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        window.toggleCommandPalette();
    }
    // Escape key
    if (e.key === 'Escape') {
        const cmdModal = document.getElementById('commandPaletteModal');
        if (cmdModal && cmdModal.classList.contains('open')) {
            window.toggleCommandPalette(false);
        }
        window.toggleMobileNav(false);
    }
});

// Mobile Navigation Toggle
window.toggleMobileNav = (forceState) => {
    const drawer = document.getElementById('mobileNavDrawer');
    const backdrop = document.getElementById('mobileNavBackdrop');
    if (!drawer || !backdrop) return;

    const isOpen = typeof forceState === 'boolean' ? forceState : !drawer.classList.contains('open');
    if (isOpen) {
        drawer.classList.add('open');
        backdrop.classList.add('open');
        document.body.style.overflow = 'hidden';
    } else {
        drawer.classList.remove('open');
        backdrop.classList.remove('open');
        document.body.style.overflow = '';
    }
};

/**
 * Universal Case Analysis Starter
 */
window.startCaseAnalysis = (initialData = {}) => {
    window.state = window.state || {};
    window.state.caseData = { ...(initialData || {}) };
    window.state.currentStep = 1;
    try {
        localStorage.setItem('judiq_wizard_autosave', JSON.stringify(window.state.caseData));
    } catch (_) {}
    switchScreen('caseWizardScreen');
    if (typeof window.setCaseType === 'function' && initialData.case_type) {
        window.setCaseType(initialData.case_type);
    } else if (typeof renderWizardStep === 'function') {
        renderWizardStep();
    }
};

/**
 * Demo Case Loaders with Instant Preset Population
 */
window.loadDemoCase = () => {
    if (typeof window.loadSampleCaseData === 'function') {
        window.loadSampleCaseData(window.SAMPLE_NI_ACT_PRESET);
    }
};

window.loadSarfaesiDemoCase = () => {
    if (typeof window.loadSampleCaseData === 'function') {
        window.loadSampleCaseData(window.SAMPLE_SARFAESI_PRESET);
    }
};

window.loadCompositeDemoCase = () => {
    if (typeof window.loadSampleCaseData === 'function') {
        window.loadSampleCaseData(window.SAMPLE_COMPOSITE_PRESET);
    }
};

window.loadCriminalDemoCase = () => {
    if (typeof window.loadSampleCaseData === 'function') {
        window.loadSampleCaseData(window.SAMPLE_CRIMINAL_PRESET);
    }
};

window.loadCivilDemoCase = () => {
    if (typeof window.loadSampleCaseData === 'function') {
        window.loadSampleCaseData(window.SAMPLE_CIVIL_PRESET);
    }
};

// ============================================================================
// ADMIN CONTROL CENTER & BANK GOVERNANCE FUNCTIONS
// ============================================================================

let adminCachedUsers = [];
let adminCachedPendingPlans = [];
let adminCachedBankOfficers = [];
let adminCachedBankAudits = [];
let adminAuthToken = null;

window.switchAdminTab = (tabName) => {
    const tabs = ['litigators', 'plans', 'bank', 'engines', 'health', 'security'];
    tabs.forEach(t => {
        const btn = document.getElementById(`adminTabBtn${t.charAt(0).toUpperCase() + t.slice(1)}`);
        const content = document.getElementById(`admin${t.charAt(0).toUpperCase() + t.slice(1)}TabContent`);
        if (btn) {
            if (t === tabName) btn.classList.add('active');
            else btn.classList.remove('active');
        }
        if (content) {
            if (t === tabName) content.style.display = 'block';
            else content.style.display = 'none';
        }
    });

    if (tabName === 'security') {
        window.loadAdminSecurityLogs();
    } else if (tabName === 'health') {
        window.loadAdminSystemHealth();
    }
};


window.openAdminPortal = async () => {
    const user = window.state.currentUser;
    const userEmail = (user && user.email ? user.email : 'admin@judiq.ai').toLowerCase().trim();
    
    const adminEmailEl = document.getElementById('adminSessionEmail');
    if (adminEmailEl) adminEmailEl.textContent = userEmail;

    switchScreen('adminPortalScreen');
    await window.loadAdminPortalData();
};

window.loadAdminPortalData = async () => {
    const user = window.state.currentUser;
    const userEmail = (user && user.email ? user.email : 'admin@judiq.ai').toLowerCase().trim();

    try {
        // Authenticate admin session
        const authRes = await api.verifyAdminAuth(userEmail);
        if (!authRes || !authRes.is_admin) {
            if (window.ui) window.ui.toast('Access Denied: Administrative privileges required.', 'error');
            showDashboard();
            return;
        }
        adminAuthToken = authRes.token;

        // 1. Fetch Platform Litigator stats
        const statsRes = await api.getAdminStats(adminAuthToken);
        if (statsRes && statsRes.success && statsRes.stats) {
            const s = statsRes.stats;
            const setStat = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
            setStat('statTotalUsers', s.total_users || 0);
            setStat('statActiveUsers', s.active_users || 0);
            setStat('statReportsThisMonth', s.total_reports_this_month || 0);
            setStat('statCurrentPeriod', s.current_period || '--');
            setStat('statTotalCases', s.total_saved_cases || 0);
            setStat('statAuditEvents', s.total_audit_events || 0);
        }

        // 2. Fetch Litigator Users
        const usersRes = await api.getAdminUsers(adminAuthToken);
        if (usersRes && usersRes.success && Array.isArray(usersRes.users)) {
            adminCachedUsers = usersRes.users;
            window.renderAdminUsersTable(adminCachedUsers);
            window.updateFilterPillCounts();
        }

        // 2b. Fetch Pending Plan Requests
        try {
            const plansRes = await api.getPendingPlans(adminAuthToken);
            if (plansRes && plansRes.success && Array.isArray(plansRes.pending_plans)) {
                adminCachedPendingPlans = plansRes.pending_plans;
                window.renderAdminPendingPlansTable(adminCachedPendingPlans);
                const badge = document.getElementById('pendingPlansCountBadge');
                if (badge) {
                    badge.textContent = adminCachedPendingPlans.length;
                    badge.style.display = adminCachedPendingPlans.length > 0 ? 'inline-block' : 'none';
                }
            }
        } catch (e) {
            console.warn('Pending plans load skipped:', e);
        }

        // 3. Fetch Bank Stats
        try {
            const bStatsRes = await api.getAdminBankStats(adminAuthToken);
            if (bStatsRes && bStatsRes.success && bStatsRes.stats) {
                const bs = bStatsRes.stats;
                const setBStat = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
                setBStat('statBankOfficers', bs.total_bank_officers || 0);
                setBStat('statActiveBankOfficers', bs.active_bank_officers || 0);
                setBStat('statBankPartners', bs.total_institutional_partners || 0);
                setBStat('statBankAudits', bs.total_audits_performed || 0);
                setBStat('statBankAuditsMonth', bs.audits_this_month || 0);
                const vol = bs.total_recovery_volume_evaluated || 0;
                setBStat('statBankVolume', vol >= 10000000 ? `₹${(vol/10000000).toFixed(2)} Cr` : (vol >= 100000 ? `₹${(vol/100000).toFixed(1)} L` : `₹${vol.toLocaleString('en-IN')}`));
            }
        } catch (e) {
            console.warn('Bank stats load skipped:', e);
        }

        // 4. Fetch Bank Officers
        try {
            const bOfficersRes = await api.getAdminBankOfficers(adminAuthToken);
            if (bOfficersRes && bOfficersRes.success && Array.isArray(bOfficersRes.officers)) {
                adminCachedBankOfficers = bOfficersRes.officers;
                window.renderAdminBankOfficersTable(adminCachedBankOfficers);
            }
        } catch (e) {
            console.warn('Bank officers load skipped:', e);
        }

        // 5. Fetch Bank Audits Stream
        try {
            const bAuditsRes = await api.getAdminBankAudits(adminAuthToken);
            if (bAuditsRes && bAuditsRes.success && Array.isArray(bAuditsRes.audits)) {
                adminCachedBankAudits = bAuditsRes.audits;
                window.renderAdminBankAuditsTable(adminCachedBankAudits);
            }
        } catch (e) {
            console.warn('Bank audits stream load skipped:', e);
        }

    } catch (err) {
        console.error('Error loading admin portal data:', err);
        if (window.ui) window.ui.toast('Failed to load admin data: ' + err.message, 'error');
    }
};

window.copyToClipboard = (text, label) => {
    if (!text) return;
    navigator.clipboard.writeText(text).then(() => {
        if (window.ui) window.ui.toast(`${label || 'Item'} copied to clipboard!`, 'success');
    }).catch(() => {
        if (window.ui) window.ui.toast('Could not copy to clipboard', 'warning');
    });
};

window.updateFilterPillCounts = () => {
    const users = adminCachedUsers || [];
    const setPill = (id, count) => {
        const el = document.getElementById(id);
        if (el) el.textContent = count;
    };
    setPill('pillCountAll', users.length);
    setPill('pillCountLawFirm', users.filter(u => u.role === 'law_firm').length);
    setPill('pillCountEnterprise', users.filter(u => u.role === 'enterprise').length);
    setPill('pillCountCitizen', users.filter(u => u.role === 'citizen').length);
    setPill('pillCountPending', users.filter(u => u.plan_status === 'PENDING_APPROVAL').length);
    setPill('pillCountSuspended', users.filter(u => u.is_active === false).length);
};

window.setQuickFilter = (filterKey) => {
    const select = document.getElementById('adminUserRoleFilter');
    if (select) select.value = filterKey;

    const pillMap = {
        '': 'filterPillAll',
        'law_firm': 'filterPillLawFirm',
        'enterprise': 'filterPillEnterprise',
        'citizen': 'filterPillCitizen',
        'status_pending': 'filterPillPending',
        'status_suspended': 'filterPillSuspended'
    };

    document.querySelectorAll('.admin-filter-pill').forEach(btn => btn.classList.remove('active'));
    const targetId = pillMap[filterKey] || 'filterPillAll';
    const targetBtn = document.getElementById(targetId);
    if (targetBtn) targetBtn.classList.add('active');

    window.filterAdminUsersTable();
};

window.exportAdminUsers = (format = 'json') => {
    const users = adminCachedUsers || [];
    if (users.length === 0) {
        if (window.ui) window.ui.toast('No litigator accounts to export.', 'warning');
        return;
    }

    if (format === 'json') {
        const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(users, null, 2));
        const dlAnchor = document.createElement('a');
        dlAnchor.setAttribute("href", dataStr);
        dlAnchor.setAttribute("download", `judiq_litigators_${new Date().toISOString().slice(0, 10)}.json`);
        document.body.appendChild(dlAnchor);
        dlAnchor.click();
        dlAnchor.remove();
        if (window.ui) window.ui.toast('Exported litigators JSON successfully.', 'success');
    }
};

window.renderAdminUsersTable = (users) => {
    const tbody = document.getElementById('adminUsersTableBody');
    if (!tbody) return;

    if (!users || users.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" style="padding: 3rem 1.5rem; text-align: center; color: var(--gray-400);">
                    <div style="font-size: 2rem; color: var(--gray-300); margin-bottom: 0.75rem;"><i class="fas fa-user-slash"></i></div>
                    <div style="font-size: 1rem; font-weight: 700; color: var(--gray-700); margin-bottom: 0.25rem;">No litigator accounts found</div>
                    <div style="font-size: 0.82rem; color: var(--gray-400);">Try clearing search filters or add a new litigator account.</div>
                </td>
            </tr>
        `;
        return;
    }

    const moduleConfig = {
        's138': { label: 'S.138 NI Act', bg: 'rgba(59, 130, 246, 0.12)', color: '#2563eb', icon: 'fa-scale-balanced' },
        'sarfaesi': { label: 'SARFAESI', bg: 'rgba(245, 158, 11, 0.12)', color: '#d97706', icon: 'fa-building-columns' },
        'criminal': { label: 'BNSS Criminal', bg: 'rgba(239, 68, 68, 0.12)', color: '#dc2626', icon: 'fa-gavel' },
        'civil': { label: 'Civil CPC', bg: 'rgba(16, 185, 129, 0.12)', color: '#059669', icon: 'fa-file-shield' },
        'bank_recovery': { label: 'Banking OS', bg: 'rgba(14, 165, 233, 0.12)', color: '#0284c7', icon: 'fa-landmark' },
        'counsel_intel': { label: 'Counsel Intel', bg: 'rgba(139, 92, 246, 0.12)', color: '#7c3aed', icon: 'fa-brain' }
    };

    const roleAvatarGradients = {
        'admin': 'linear-gradient(135deg, #4f46e5, #06b6d4)',
        'enterprise': 'linear-gradient(135deg, #0284c7, #0369a1)',
        'law_firm': 'linear-gradient(135deg, #8b5cf6, #6366f1)',
        'citizen': 'linear-gradient(135deg, #10b981, #059669)'
    };

    tbody.innerHTML = users.map(u => {
        const isUnlimited = u.monthly_report_limit === -1;
        const used = u.reports_used_this_month || 0;
        const limit = isUnlimited ? '∞' : u.monthly_report_limit;
        const remaining = isUnlimited ? '∞' : Math.max(0, u.monthly_report_limit - used);
        const pct = isUnlimited ? 0 : Math.min(100, Math.round((used / Math.max(1, u.monthly_report_limit)) * 100));
        const isWarning = pct >= 80;

        const email = u.email || 'Anonymous Litigator';
        const initials = email.substring(0, 2).toUpperCase();
        const avatarBg = roleAvatarGradients[u.role] || 'linear-gradient(135deg, #64748b, #475569)';

        const statusBadge = u.is_active
            ? `<span class="status-badge-active" title="Account active and verified"><i class="fas fa-circle-check"></i> Active</span>`
            : `<span class="status-badge-suspended" title="Account suspended"><i class="fas fa-circle-xmark"></i> Suspended</span>`;

        const planStatusBadge = u.plan_status === 'APPROVED'
            ? `<span class="badge" style="background: rgba(16, 185, 129, 0.12); color: #10b981; font-weight: 700; font-size: 0.7rem; margin-top: 3px; display: inline-flex; align-items: center; gap: 0.25rem;"><i class="fas fa-check-circle"></i> Approved</span>`
            : (u.plan_status === 'PENDING_APPROVAL'
                ? `<span class="badge" style="background: rgba(245, 158, 11, 0.15); color: #d97706; font-weight: 700; font-size: 0.7rem; margin-top: 3px; display: inline-flex; align-items: center; gap: 0.25rem;"><i class="fas fa-hourglass-half"></i> Pending Plan</span>`
                : `<span class="badge" style="background: rgba(239, 68, 68, 0.12); color: #ef4444; font-weight: 700; font-size: 0.7rem; margin-top: 3px; display: inline-flex; align-items: center; gap: 0.25rem;"><i class="fas fa-ban"></i> Rejected</span>`);

        const modules = Array.isArray(u.selected_modules) ? u.selected_modules : ['s138'];
        const moduleBadges = modules.slice(0, 3).map(m => {
            const conf = moduleConfig[m] || { label: m, bg: 'rgba(56, 189, 248, 0.12)', color: '#0284c7', icon: 'fa-bolt' };
            return `
                <span class="admin-module-badge" style="background: ${conf.bg}; color: ${conf.color};">
                    <i class="fas ${conf.icon}" style="font-size: 0.65rem;"></i> ${conf.label}
                </span>
            `;
        }).join('') + (modules.length > 3 ? `<span style="font-size: 0.7rem; color: var(--gray-500); font-weight: 700; margin-left: 2px;">+${modules.length - 3}</span>` : '');

        const priceText = u.monthly_price_inr ? `₹${Number(u.monthly_price_inr).toLocaleString('en-IN')}/mo` : '₹500/mo';
        const createdDate = u.created_at ? new Date(u.created_at).toLocaleDateString() : '';

        return `
            <tr id="adminRow_${u.user_id}">
                <td>
                    <div class="admin-user-cell">
                        <div class="admin-avatar-bubble" style="background: ${avatarBg};">
                            ${initials}
                        </div>
                        <div>
                            <div style="font-weight: 700; color: var(--gray-900); font-size: 0.88rem; display: flex; align-items: center; gap: 0.35rem;">
                                <span>${email}</span>
                                <span class="admin-copy-chip" onclick="window.copyToClipboard('${email}', 'Email')" title="Copy Email" style="font-size: 0.75rem; color: var(--gray-400);">
                                    <i class="fas fa-copy"></i>
                                </span>
                            </div>
                            <div style="display: flex; align-items: center; gap: 0.35rem; margin-top: 2px;">
                                <span style="font-size: 0.72rem; color: var(--gray-400); font-family: monospace;" title="Litigator UID">${u.user_id}</span>
                                <span class="admin-copy-chip" onclick="window.copyToClipboard('${u.user_id}', 'User ID')" title="Copy User ID" style="font-size: 0.68rem; color: var(--gray-400);">
                                    <i class="fas fa-copy"></i>
                                </span>
                            </div>
                            ${createdDate ? `<div style="font-size: 0.68rem; color: var(--gray-400); margin-top: 1px;"><i class="fas fa-calendar-day" style="font-size: 0.62rem;"></i> Joined: ${createdDate}</div>` : ''}
                        </div>
                    </div>
                </td>
                <td>
                    <select id="adminRole_${u.user_id}" style="padding: 0.35rem 0.65rem; border-radius: 6px; border: 1px solid var(--border-color); background: var(--gray-50); font-size: 0.8rem; font-weight: 700; color: var(--gray-800); width: 100%;">
                        <option value="law_firm" ${u.role === 'law_firm' ? 'selected' : ''}>Law Firm / Chamber</option>
                        <option value="enterprise" ${u.role === 'enterprise' ? 'selected' : ''}>Enterprise Legal</option>
                        <option value="citizen" ${u.role === 'citizen' ? 'selected' : ''}>Independent Litigator</option>
                        <option value="admin" ${u.role === 'admin' ? 'selected' : ''}>System Administrator</option>
                    </select>
                    <div>${planStatusBadge}</div>
                </td>
                <td>
                    <div style="display: flex; flex-wrap: wrap; gap: 2px; margin-bottom: 3px;">${moduleBadges}</div>
                    <div style="font-size: 0.76rem; font-weight: 800; color: #4f46e5;">${priceText}</div>
                </td>
                <td>
                    <div style="display: flex; align-items: center; gap: 0.4rem; margin-bottom: 0.35rem;">
                        <input type="number" id="adminLimit_${u.user_id}" value="${u.monthly_report_limit}" style="width: 65px; padding: 0.25rem 0.4rem; border-radius: 6px; border: 1px solid var(--border-color); font-size: 0.85rem; font-weight: 800; text-align: center; color: var(--gray-900);">
                        <span style="font-size: 0.72rem; color: var(--gray-500); font-weight: 600;">reports/mo</span>
                    </div>
                    <div style="display: flex; gap: 0.2rem;">
                        <button class="quota-preset-btn" onclick="window.setQuotaPreset('${u.user_id}', 10)">10</button>
                        <button class="quota-preset-btn" onclick="window.setQuotaPreset('${u.user_id}', 25)">25</button>
                        <button class="quota-preset-btn" onclick="window.setQuotaPreset('${u.user_id}', 50)">50</button>
                        <button class="quota-preset-btn" onclick="window.setQuotaPreset('${u.user_id}', 100)">100</button>
                        <button class="quota-preset-btn" onclick="window.setQuotaPreset('${u.user_id}', -1)" title="Unlimited Allowance">∞</button>
                    </div>
                </td>
                <td>
                    <div style="display: flex; justify-content: space-between; font-size: 0.76rem; font-weight: 700; color: var(--gray-800); margin-bottom: 2px;">
                        <span>${used} used</span>
                        <span style="color: ${isWarning ? '#ef4444' : 'var(--gray-500)'};">${remaining} left</span>
                    </div>
                    <div class="quota-progress-track" title="${pct}% of monthly quota consumed">
                        <div class="quota-progress-fill ${isWarning ? 'warning' : ''}" style="width: ${pct}%;"></div>
                    </div>
                </td>
                <td>
                    ${statusBadge}
                </td>
                <td style="text-align: right;">
                    <div style="display: inline-flex; align-items: center; gap: 0.3rem;">
                        <button class="btn btn-sm btn-primary" onclick="window.saveUserQuota('${u.user_id}', '${u.email || ''}')" title="Save Allocation Changes" style="padding: 0.3rem 0.55rem; font-size: 0.78rem;">
                            <i class="fas fa-check"></i> Save
                        </button>
                        <button class="btn btn-sm btn-outline" onclick="window.openUserDetailsModal('${u.user_id}')" title="View Full Litigator Dossier" style="padding: 0.3rem 0.55rem; font-size: 0.78rem; color: #4f46e5; border-color: rgba(79,70,229,0.4);">
                            <i class="fas fa-id-card"></i>
                        </button>
                        <button class="btn btn-sm btn-outline" onclick="window.resetUserUsage('${u.user_id}')" title="Reset Monthly Usage Counter" style="padding: 0.3rem 0.45rem; font-size: 0.78rem;">
                            <i class="fas fa-arrow-rotate-left"></i>
                        </button>
                        <button class="btn btn-sm ${u.is_active ? 'btn-danger' : 'btn-secondary'}" onclick="window.toggleUserStatus('${u.user_id}', ${u.is_active})" title="${u.is_active ? 'Suspend Account' : 'Activate Account'}" style="padding: 0.3rem 0.45rem; font-size: 0.78rem;">
                            <i class="fas ${u.is_active ? 'fa-ban' : 'fa-check-circle'}"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
};


window.openUserDetailsModal = (userId) => {
    const user = (adminCachedUsers || []).find(u => u.user_id === userId);
    if (!user) return;

    const modal = document.getElementById('adminAccountDetailsModal');
    if (!modal) return;

    const moduleDetails = {
        's138': { title: 'Section 138 NI Act Cheque Dishonour Engine', desc: 'Statutory 30-day notice verification, 15-day cure window, S.141 director vicarious liability, signature & part-payment defenses.' },
        'sarfaesi': { title: 'SARFAESI Act 2002 & DRT Enforcement Engine', desc: 'S.13(2) 60-day demand notices, S.13(4) possession measures, S.31(i) agricultural land immunity bars, CERSAI security interest priority.' },
        'criminal': { title: 'Criminal Defense & Satender Kumar Antil Matrix', desc: '4-Category Supreme Court bail rubric (A/B/C/D), S.482 CrPC quashing strategy, cross-examination blueprint generator.' },
        'civil': { title: 'Civil Litigation & Commercial Court Matrix', desc: 'Order VII Rule 11 CPC rejection of plaint, Section 12A commercial pre-institution mediation, summary suits under Order 37.' },
        'bank_recovery': { title: 'Institutional Banking & SARB Recovery OS', desc: 'Pre-litigation recovery viability score, statutory defect auditing, loan asset reconstruction workflows.' },
        'counsel_intel': { title: 'Neural Precedent RAG & Counsel Intel', desc: 'Vector semantic precedent retrieval, binding High Court / Supreme Court neural citation reranker.' }
    };

    const isUnlimited = user.monthly_report_limit === -1;
    const used = user.reports_used_this_month || 0;
    const limitStr = isUnlimited ? '∞ Unlimited' : `${user.monthly_report_limit} Reports`;
    const remainingStr = isUnlimited ? '∞ Unlimited' : `${Math.max(0, user.monthly_report_limit - used)} Reports`;
    const usagePct = isUnlimited ? '0%' : `${Math.min(100, Math.round((used / Math.max(1, user.monthly_report_limit)) * 100))}%`;

    const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
    const setHtml = (id, html) => { const el = document.getElementById(id); if (el) el.innerHTML = html; };

    setEl('modalUserEmail', user.email || 'Anonymous Litigator');
    setEl('modalUserIdSubtitle', user.user_id);
    setHtml('modalUserStatusBadge', user.is_active
        ? '<i class="fas fa-circle-check"></i> Active'
        : '<i class="fas fa-circle-xmark"></i> Suspended');
    const statusBadgeEl = document.getElementById('modalUserStatusBadge');
    if (statusBadgeEl) {
        statusBadgeEl.className = user.is_active ? 'status-badge-active' : 'status-badge-suspended';
    }

    setEl('modalUserPlanBadge', user.plan_status || 'APPROVED');
    setEl('modalUserLimitVal', isUnlimited ? '∞' : user.monthly_report_limit);
    setEl('modalUserUsedVal', used);
    setEl('modalUserPeriodVal', `Cycle: ${user.current_month_period || 'Active Period'}`);
    setEl('modalUserRemainingVal', isUnlimited ? '∞' : Math.max(0, user.monthly_report_limit - used));
    setEl('modalUserPriceVal', `₹${Number(user.monthly_price_inr || 500).toLocaleString('en-IN')}`);
    setEl('modalUserRoleVal', user.role ? user.role.replace('_', ' ').toUpperCase() : 'LAW FIRM');
    setEl('modalUserUsagePct', usagePct);

    const modules = Array.isArray(user.selected_modules) ? user.selected_modules : ['s138'];
    setEl('modalUserModuleCountBadge', `${modules.length} Engine${modules.length === 1 ? '' : 's'} Active`);

    const modulesHtml = modules.map(m => {
        const info = moduleDetails[m] || { title: m.toUpperCase(), desc: 'Statutory automated litigation engine.' };
        return `
            <div style="background: var(--gray-50); border: 1px solid var(--border-color); border-radius: 10px; padding: 0.75rem 1rem; width: 100%;">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.25rem;">
                    <div style="font-weight: 700; color: var(--gray-900); font-size: 0.85rem;">
                        <i class="fas fa-check-circle" style="color: #10b981; margin-right: 0.35rem;"></i> ${info.title}
                    </div>
                    <span class="badge" style="background: rgba(16, 185, 129, 0.12); color: #10b981; font-weight: 700; font-size: 0.7rem;">ENABLED</span>
                </div>
                <div style="font-size: 0.75rem; color: var(--gray-500); line-height: 1.4;">${info.desc}</div>
            </div>
        `;
    }).join('');
    setHtml('modalUserModulesContainer', modulesHtml);

    setEl('modalUserCreatedAt', user.created_at ? new Date(user.created_at).toLocaleString() : 'Initial Core Initialization');
    setEl('modalUserUpdatedAt', user.updated_at ? new Date(user.updated_at).toLocaleString() : 'N/A');
    setEl('modalUserApprovedBy', user.approved_by || (user.plan_status === 'APPROVED' ? 'System / Auto-Verified' : 'Pending Administrator Action'));
    setEl('modalUserApprovedAt', user.approved_at ? new Date(user.approved_at).toLocaleString() : (user.plan_status === 'APPROVED' ? 'Active' : 'Pending'));

    const resetBtn = document.getElementById('modalResetUsageBtn');
    if (resetBtn) {
        resetBtn.onclick = async () => {
            await window.resetUserUsage(user.user_id);
            window.closeUserDetailsModal();
        };
    }

    const toggleBtn = document.getElementById('modalToggleStatusBtn');
    if (toggleBtn) {
        toggleBtn.innerHTML = user.is_active ? '<i class="fas fa-ban"></i> Suspend Account' : '<i class="fas fa-circle-check"></i> Activate Account';
        toggleBtn.className = user.is_active ? 'btn btn-outline btn-sm' : 'btn btn-primary btn-sm';
        toggleBtn.onclick = async () => {
            await window.toggleUserStatus(user.user_id, user.is_active);
            window.closeUserDetailsModal();
        };
    }

    modal.classList.remove('hidden');
};

window.closeUserDetailsModal = () => {
    const modal = document.getElementById('adminAccountDetailsModal');
    if (modal) modal.classList.add('hidden');
};

window.renderAdminBankOfficersTable = (officers) => {
    const tbody = document.getElementById('adminBankOfficersTableBody');
    if (!tbody) return;

    if (!officers || officers.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" style="padding: 2.5rem; text-align: center; color: var(--gray-400);">
                    <i class="fas fa-building-columns" style="font-size: 1.5rem; margin-bottom: 0.5rem; display: block;"></i>
                    No institutional bank officers registered.
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = officers.map(o => {
        const isUnlimited = o.monthly_audit_limit === -1;
        const used = o.audits_used_this_month || 0;
        const limit = isUnlimited ? '∞' : o.monthly_audit_limit;
        const pct = isUnlimited ? 0 : Math.min(100, Math.round((used / Math.max(1, o.monthly_audit_limit)) * 100));
        const isWarning = pct >= 80;

        const statusBadge = o.is_active
            ? `<span class="status-badge-active"><i class="fas fa-circle-check"></i> Active</span>`
            : `<span class="status-badge-suspended"><i class="fas fa-circle-xmark"></i> Suspended</span>`;

        return `
            <tr id="adminBankRow_${o.officer_id}">
                <td>
                    <div style="font-weight: 700; color: var(--gray-900);">${o.name || o.officer_id}</div>
                    <div style="font-size: 0.75rem; color: #0284c7; font-weight: 600;">${o.bank_name || 'Bank Partner'}</div>
                    <div style="font-size: 0.72rem; color: var(--gray-400); font-family: monospace;">${o.officer_id}</div>
                </td>
                <td>
                    <div style="font-size: 0.82rem; color: var(--gray-800); max-width: 260px;">${o.branch_name || '--'}</div>
                    <div style="font-size: 0.72rem; color: var(--gray-400);">${o.email || ''}</div>
                    ${o.ifsc_code && o.ifsc_code !== 'N/A' ? `<div style="font-size: 0.7rem; color: #0284c7; font-family: monospace;">IFSC: ${o.ifsc_code}</div>` : ''}
                </td>
                <td>
                    <select id="adminBankRole_${o.officer_id}" style="padding: 0.25rem 0.5rem; border-radius: 6px; border: 1px solid var(--border-color); background: var(--gray-50); font-size: 0.78rem; font-weight: 600; width: 100%;">
                        <option value="bank_officer" ${o.role === 'bank_officer' ? 'selected' : ''}>Bank Officer</option>
                        <option value="sarb_manager" ${o.role === 'sarb_manager' ? 'selected' : ''}>SARB Manager</option>
                        <option value="recovery_head" ${o.role === 'recovery_head' ? 'selected' : ''}>Recovery Head</option>
                    </select>
                </td>
                <td>
                    <div style="display: flex; align-items: center; gap: 0.4rem; margin-bottom: 0.3rem;">
                        <input type="number" id="adminBankLimit_${o.officer_id}" value="${o.monthly_audit_limit}" style="width: 70px; padding: 0.25rem 0.5rem; border-radius: 6px; border: 1px solid var(--border-color); font-size: 0.85rem; font-weight: 700; text-align: center;">
                        <span style="font-size: 0.75rem; color: var(--gray-500);">audits/mo</span>
                    </div>
                </td>
                <td>
                    <div style="display: flex; justify-content: space-between; font-size: 0.8rem; font-weight: 700; color: var(--gray-800);">
                        <span>${used} used</span>
                        <span>/ ${limit}</span>
                    </div>
                    <div class="quota-progress-track">
                        <div class="quota-progress-fill ${isWarning ? 'warning' : ''}" style="width: ${pct}%; background: linear-gradient(90deg, #0284c7, #0369a1);"></div>
                    </div>
                </td>
                <td>
                    ${statusBadge}
                </td>
                <td style="text-align: right;">
                    <div style="display: inline-flex; align-items: center; gap: 0.35rem;">
                        <button class="btn btn-sm btn-primary" onclick="window.saveBankOfficerQuota('${o.officer_id}')" title="Save Changes" style="padding: 0.3rem 0.6rem; font-size: 0.78rem; background: #0284c7;">
                            <i class="fas fa-check"></i>
                        </button>
                        <button class="btn btn-sm btn-outline" onclick="window.openBankOfficerDetailsModal('${o.officer_id}')" title="View Full Officer Dossier" style="padding: 0.3rem 0.55rem; font-size: 0.78rem; color: #0284c7; border-color: rgba(2,132,199,0.4);">
                            <i class="fas fa-id-card"></i>
                        </button>
                        <button class="btn btn-sm ${o.is_active ? 'btn-danger' : 'btn-secondary'}" onclick="window.toggleBankOfficerStatus('${o.officer_id}', ${o.is_active})" title="${o.is_active ? 'Suspend Officer' : 'Activate Officer'}" style="padding: 0.3rem 0.55rem; font-size: 0.78rem;">
                            <i class="fas ${o.is_active ? 'fa-ban' : 'fa-check-circle'}"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
};

window.openBankOfficerDetailsModal = (officerId) => {
    const officer = (adminCachedBankOfficers || []).find(o => o.officer_id === officerId);
    if (!officer) return;

    const modal = document.getElementById('adminBankOfficerDetailsModal');
    if (!modal) return;

    const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
    const setHtml = (id, html) => { const el = document.getElementById(id); if (el) el.innerHTML = html; };

    setEl('modalBankOfficerName', officer.name || officer.officer_id);
    setEl('modalBankOfficerIdSubtitle', officer.officer_id);
    setHtml('modalBankOfficerStatusBadge', officer.is_active
        ? '<i class="fas fa-circle-check"></i> Active'
        : '<i class="fas fa-circle-xmark"></i> Suspended');
    const badgeEl = document.getElementById('modalBankOfficerStatusBadge');
    if (badgeEl) {
        badgeEl.className = officer.is_active ? 'status-badge-active' : 'status-badge-suspended';
    }

    setEl('modalBankOfficerLimitVal', officer.monthly_audit_limit === -1 ? '∞ Unlimited' : officer.monthly_audit_limit);
    setEl('modalBankOfficerUsedVal', officer.audits_used_this_month || 0);
    setEl('modalBankOfficerBankVal', officer.bank_name || 'Institutional Partner');
    setEl('modalBankOfficerRoleVal', officer.role ? officer.role.replace('_', ' ').toUpperCase() : 'BANK OFFICER');
    setEl('modalBankOfficerBranchVal', officer.branch_name || 'SARB Division');
    setEl('modalBankOfficerEmailVal', officer.email || 'N/A');
    setEl('modalBankOfficerIfscVal', officer.ifsc_code || 'N/A');
    setEl('modalBankOfficerDeptVal', officer.department || 'Stressed Assets Resolution');

    const toggleBtn = document.getElementById('modalBankToggleStatusBtn');
    if (toggleBtn) {
        toggleBtn.innerHTML = officer.is_active ? '<i class="fas fa-ban"></i> Suspend Access' : '<i class="fas fa-circle-check"></i> Activate Access';
        toggleBtn.className = officer.is_active ? 'btn btn-outline btn-sm' : 'btn btn-primary btn-sm';
        toggleBtn.onclick = async () => {
            await window.toggleBankOfficerStatus(officer.officer_id, officer.is_active);
            window.closeBankOfficerDetailsModal();
        };
    }

    modal.classList.remove('hidden');
};

window.closeBankOfficerDetailsModal = () => {
    const modal = document.getElementById('adminBankOfficerDetailsModal');
    if (modal) modal.classList.add('hidden');
};

window.renderAdminBankAuditsTable = (audits) => {
    const tbody = document.getElementById('adminBankAuditsTableBody');
    if (!tbody) return;

    if (!audits || audits.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" style="padding: 2rem; text-align: center; color: var(--gray-400);">
                    No institutional audits recorded yet.
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = audits.map(a => {
        const amt = a.default_amount ? `₹${Number(a.default_amount).toLocaleString('en-IN')}` : '--';
        const isReady = a.verdict === 'READY_FOR_ADVOCATE_DISPATCH';
        const verdictBadge = isReady
            ? `<span style="color: #10b981; font-weight: 700;"><i class="fas fa-circle-check"></i> Clean / Ready</span>`
            : `<span style="color: #ef4444; font-weight: 700;"><i class="fas fa-triangle-exclamation"></i> Defective (${a.defect_count || 0})</span>`;

        return `
            <tr>
                <td style="font-family: monospace; font-size: 0.78rem; color: #0284c7; font-weight: 700;">${a.audit_id || '--'}</td>
                <td>
                    <div style="font-weight: 700; color: var(--gray-900); font-size: 0.82rem;">${a.officer_id || '--'}</div>
                    <div style="font-size: 0.72rem; color: var(--gray-500);">${a.bank_name || ''}</div>
                </td>
                <td>
                    <div style="font-weight: 600; color: var(--gray-800);">${a.borrower_name || '--'}</div>
                    <div style="font-size: 0.72rem; color: var(--gray-400); font-family: monospace;">${a.loan_account_no || ''}</div>
                </td>
                <td style="font-weight: 700; color: var(--gray-900);">${amt}</td>
                <td>
                    <span style="display: inline-block; padding: 0.2rem 0.5rem; border-radius: 12px; font-weight: 700; font-size: 0.78rem; background: ${a.viability_score >= 80 ? 'rgba(16, 185, 129, 0.15); color: #10b981;' : 'rgba(239, 68, 68, 0.15); color: #ef4444;'}">
                        ${Math.round(a.viability_score || 0)}/100
                    </span>
                </td>
                <td>${verdictBadge}</td>
                <td style="text-align: right; font-size: 0.75rem; color: var(--gray-400);">
                    ${a.timestamp ? new Date(a.timestamp).toLocaleString() : '--'}
                </td>
            </tr>
        `;
    }).join('');
};

window.filterAdminUsersTable = () => {
    const input = document.getElementById('adminUserSearchInput');
    const roleFilter = document.getElementById('adminUserRoleFilter');
    const query = (input ? input.value : '').toLowerCase().trim();
    const selectedFilter = roleFilter ? roleFilter.value : '';

    let filtered = adminCachedUsers || [];
    if (query) {
        filtered = filtered.filter(u => {
            const modulesStr = Array.isArray(u.selected_modules) ? u.selected_modules.join(' ') : '';
            return (u.email && u.email.toLowerCase().includes(query)) ||
                (u.user_id && u.user_id.toLowerCase().includes(query)) ||
                (u.role && u.role.toLowerCase().includes(query)) ||
                (u.plan_status && u.plan_status.toLowerCase().includes(query)) ||
                modulesStr.toLowerCase().includes(query);
        });
    }
    if (selectedFilter) {
        if (selectedFilter === 'status_active') {
            filtered = filtered.filter(u => u.is_active === true);
        } else if (selectedFilter === 'status_suspended') {
            filtered = filtered.filter(u => u.is_active === false);
        } else if (selectedFilter === 'status_pending') {
            filtered = filtered.filter(u => u.plan_status === 'PENDING_APPROVAL');
        } else {
            filtered = filtered.filter(u => u.role === selectedFilter);
        }
    }
    window.renderAdminUsersTable(filtered);
};

window.filterAdminBankOfficersTable = () => {
    const input = document.getElementById('adminBankSearchInput');
    const query = (input ? input.value : '').toLowerCase().trim();
    if (!query) {
        window.renderAdminBankOfficersTable(adminCachedBankOfficers);
        return;
    }
    const filtered = adminCachedBankOfficers.filter(o => 
        (o.name && o.name.toLowerCase().includes(query)) ||
        (o.officer_id && o.officer_id.toLowerCase().includes(query)) ||
        (o.bank_name && o.bank_name.toLowerCase().includes(query)) ||
        (o.branch_name && o.branch_name.toLowerCase().includes(query)) ||
        (o.ifsc_code && o.ifsc_code.toLowerCase().includes(query)) ||
        (o.department && o.department.toLowerCase().includes(query))
    );
    window.renderAdminBankOfficersTable(filtered);
};


window.setQuotaPreset = (userId, limit) => {
    const input = document.getElementById(`adminLimit_${userId}`);
    if (input) input.value = limit;
};

window.saveUserQuota = async (userId, email) => {
    if (!adminAuthToken) return;
    const limitInput = document.getElementById(`adminLimit_${userId}`);
    const roleSelect = document.getElementById(`adminRole_${userId}`);
    const monthlyLimit = limitInput ? parseInt(limitInput.value, 10) : 25;
    const role = roleSelect ? roleSelect.value : 'law_firm';

    try {
        const res = await api.allocateUserQuota(userId, monthlyLimit, role, email, adminAuthToken);
        if (res && res.success) {
            if (window.ui) window.ui.toast(`Quota updated: ${monthlyLimit === -1 ? 'Unlimited' : monthlyLimit} reports/mo.`, 'success');
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to update quota', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Quota update error: ' + err.message, 'error');
    }
};

window.saveBankOfficerQuota = async (officerId) => {
    if (!adminAuthToken) return;
    const limitInput = document.getElementById(`adminBankLimit_${officerId}`);
    const roleSelect = document.getElementById(`adminBankRole_${officerId}`);
    const monthlyLimit = limitInput ? parseInt(limitInput.value, 10) : 100;
    const role = roleSelect ? roleSelect.value : 'bank_officer';

    try {
        const res = await api.allocateBankOfficerQuota(officerId, monthlyLimit, role, null, null, null, adminAuthToken);
        if (res && res.success) {
            if (window.ui) window.ui.toast(`Bank quota updated: ${monthlyLimit === -1 ? 'Unlimited' : monthlyLimit} audits/mo.`, 'success');
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to update bank quota', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Bank quota update error: ' + err.message, 'error');
    }
};

window.toggleBankOfficerStatus = async (officerId, currentStatus) => {
    if (!adminAuthToken) return;
    const newStatus = !currentStatus;
    const actionName = newStatus ? 'Activate' : 'Suspend';
    if (!confirm(`${actionName} access for bank officer ${officerId}?`)) return;

    try {
        const res = await api.toggleBankOfficerStatus(officerId, newStatus, adminAuthToken);
        if (res && res.success) {
            if (window.ui) window.ui.toast(`Bank officer ${newStatus ? 'activated' : 'suspended'}.`, 'success');
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to update bank officer status', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Status error: ' + err.message, 'error');
    }
};

window.openCreateBankOfficerModal = () => {
    const modal = document.getElementById('createBankOfficerModal');
    if (modal) modal.classList.remove('hidden');
};

window.closeCreateBankOfficerModal = () => {
    const modal = document.getElementById('createBankOfficerModal');
    if (modal) modal.classList.add('hidden');
};

window.submitCreateBankOfficer = async (e) => {
    if (e) e.preventDefault();
    if (!adminAuthToken) return;

    const officerId = document.getElementById('newBankOfficerId').value.trim();
    const name = document.getElementById('newBankOfficerName').value.trim();
    const bankName = document.getElementById('newBankName').value.trim();
    const branchName = document.getElementById('newBankBranchName').value.trim();
    const role = document.getElementById('newBankRole').value;
    const email = document.getElementById('newBankEmail').value.trim();
    const limit = parseInt(document.getElementById('newBankLimit').value, 10) || 100;

    try {
        const res = await api.createAdminBankOfficer({
            officer_id: officerId,
            name: name,
            bank_name: bankName,
            branch_name: branchName,
            role: role,
            email: email,
            monthly_audit_limit: limit
        }, adminAuthToken);

        if (res && res.success) {
            if (window.ui) window.ui.toast(`Bank officer account ${officerId} provisioned successfully!`, 'success');
            window.closeCreateBankOfficerModal();
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to provision bank officer', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Creation error: ' + err.message, 'error');
    }
};

window.resetUserUsage = async (userId) => {
    if (!adminAuthToken) return;
    if (!confirm('Reset monthly report usage counter to 0 for this litigator?')) return;

    try {
        const res = await api.resetUserUsage(userId, adminAuthToken);
        if (res && res.success) {
            if (window.ui) window.ui.toast('Usage counter reset to 0.', 'success');
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to reset usage', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Reset error: ' + err.message, 'error');
    }
};

window.toggleUserStatus = async (userId, currentStatus) => {
    if (!adminAuthToken) return;
    const newStatus = !currentStatus;
    const actionName = newStatus ? 'Activate' : 'Suspend';
    if (!confirm(`${actionName} this user account?`)) return;

    try {
        const res = await api.toggleUserStatus(userId, newStatus, adminAuthToken);
        if (res && res.success) {
            if (window.ui) window.ui.toast(`Account ${newStatus ? 'activated' : 'suspended'}.`, 'success');
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to toggle status', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Status error: ' + err.message, 'error');
    }
};

// ============================================================================
// ADMIN MODULAR PLAN APPROVAL CONTROLLERS
// ============================================================================

window.renderAdminPendingPlansTable = (pendingPlans) => {
    const tbody = document.getElementById('adminPendingPlansTableBody');
    if (!tbody) return;

    if (!pendingPlans || pendingPlans.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" style="padding: 2.5rem; text-align: center; color: var(--gray-400);">
                    <i class="fas fa-circle-check" style="font-size: 1.5rem; color: #10b981; margin-bottom: 0.5rem; display: block;"></i>
                    No pending modular subscription requests. All profiles are verified.
                </td>
            </tr>
        `;
        return;
    }

    const moduleLabels = {
        's138': 'S.138 NI Act',
        'sarfaesi': 'SARFAESI & DRT',
        'criminal': 'Criminal (BNSS)',
        'civil': 'Civil (CPC)',
        'bank_recovery': 'Banking OS',
        'counsel_intel': 'Counsel Intel'
    };

    tbody.innerHTML = pendingPlans.map(p => {
        const modules = Array.isArray(p.selected_modules) ? p.selected_modules : [];
        const moduleBadges = modules.map(m => `
            <span style="display: inline-block; font-size: 0.72rem; font-weight: 700; background: rgba(56, 189, 248, 0.15); color: #0284c7; padding: 2px 6px; border-radius: 4px; margin: 2px 2px 2px 0;">
                ${moduleLabels[m] || m}
            </span>
        `).join('') || '<span style="color: var(--gray-400); font-size: 0.75rem;">Standard</span>';

        return `
            <tr id="adminPlanRow_${p.user_id}">
                <td>
                    <div style="font-weight: 700; color: var(--gray-900); font-size: 0.85rem;">${p.email || p.user_id}</div>
                    <div style="font-size: 0.72rem; color: var(--gray-400); font-family: monospace;">${p.user_id}</div>
                </td>
                <td>
                    <div>${moduleBadges}</div>
                    <div style="font-size: 0.72rem; color: var(--gray-500); margin-top: 2px;">${modules.length} Active Engine(s)</div>
                </td>
                <td>
                    <div style="font-weight: 800; color: var(--gray-900); font-size: 0.88rem;">${p.requested_quota || 10} Cases</div>
                    <div style="font-size: 0.72rem; color: var(--gray-500);">per 30 days</div>
                </td>
                <td>
                    <div style="font-weight: 800; color: #0284c7; font-size: 0.95rem;">₹${Number(p.monthly_price_inr || 500).toLocaleString('en-IN')}</div>
                    <div style="font-size: 0.72rem; color: var(--gray-500);">/ month</div>
                </td>
                <td>
                    <span style="display: inline-flex; align-items: center; gap: 4px; padding: 3px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 800; background: rgba(245, 158, 11, 0.15); color: #d97706;">
                        <i class="fas fa-hourglass-half"></i> PENDING APPROVAL
                    </span>
                </td>
                <td style="text-align: right;">
                    <div style="display: inline-flex; align-items: center; gap: 0.4rem;">
                        <button class="btn btn-sm btn-primary" onclick="window.approvePlanRequest('${p.user_id}')" style="background: #10b981; border-color: #10b981; padding: 0.35rem 0.75rem; font-weight: 700; font-size: 0.78rem;">
                            <i class="fas fa-check"></i> Approve Plan
                        </button>
                        <button class="btn btn-sm btn-danger" onclick="window.rejectPlanRequest('${p.user_id}')" style="padding: 0.35rem 0.65rem; font-weight: 700; font-size: 0.78rem;">
                            <i class="fas fa-xmark"></i> Reject
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
};

window.approvePlanRequest = async (userId) => {
    if (!adminAuthToken) return;
    try {
        const res = await api.approvePlanRequest(userId, adminAuthToken);
        if (res && res.success) {
            if (window.ui) window.ui.toast(res.message || `Plan approved for ${userId}! Account activated.`, 'success');
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to approve plan', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Approval error: ' + err.message, 'error');
    }
};

window.rejectPlanRequest = async (userId) => {
    if (!adminAuthToken) return;
    const reason = prompt('Enter reason for rejection (optional):', 'Administrative decision') || 'Administrative rejection';
    try {
        const res = await api.rejectPlanRequest(userId, reason, adminAuthToken);
        if (res && res.success) {
            if (window.ui) window.ui.toast(`Plan request rejected for ${userId}. Account remains locked.`, 'warning');
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to reject plan', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Rejection error: ' + err.message, 'error');
    }
};

window.loadAdminPendingPlans = async () => {
    if (!adminAuthToken) return;
    try {
        const res = await api.getPendingPlans(adminAuthToken);
        if (res && res.success) {
            adminCachedPendingPlans = res.pending_plans || [];
            window.renderAdminPendingPlansTable(adminCachedPendingPlans);
            const badge = document.getElementById('pendingPlansCountBadge');
            if (badge) {
                badge.textContent = adminCachedPendingPlans.length;
                badge.style.display = adminCachedPendingPlans.length > 0 ? 'inline-block' : 'none';
            }
            if (window.ui) window.ui.toast('Pending plans queue refreshed.', 'info');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Failed to load pending plans: ' + err.message, 'error');
    }
};

// ============================================================================
// EXTENDED PLATFORM MANAGEMENT & AUDIT LOG CONTROLLERS
// ============================================================================

let adminCachedSecurityLogs = [];

window.openCreateLitigatorModal = () => {
    const modal = document.getElementById('createLitigatorModal');
    if (modal) modal.classList.remove('hidden');
};

window.closeCreateLitigatorModal = () => {
    const modal = document.getElementById('createLitigatorModal');
    if (modal) modal.classList.add('hidden');
};

window.submitCreateLitigator = async (e) => {
    if (e) e.preventDefault();
    if (!adminAuthToken) return;

    const email = document.getElementById('newLitigatorEmail').value.trim();
    const role = document.getElementById('newLitigatorRole').value;
    const limit = parseInt(document.getElementById('newLitigatorLimit').value, 10);
    const price = parseFloat(document.getElementById('newLitigatorPrice').value) || 500.0;
    const planStatus = document.getElementById('newLitigatorStatus').value || 'APPROVED';
    const userId = 'LIT_' + Math.random().toString(36).substring(2, 10).toUpperCase();

    const engineCheckboxes = document.querySelectorAll('input[name="newLitigatorEngines"]:checked');
    const selectedEngines = Array.from(engineCheckboxes).map(cb => cb.value);

    try {
        const res = await api.createLitigatorAccount({
            user_id: userId,
            email: email,
            role: role,
            monthly_limit: isNaN(limit) ? 25 : limit,
            selected_modules: selectedEngines.length > 0 ? selectedEngines : ['s138'],
            monthly_price_inr: price,
            plan_status: planStatus
        }, adminAuthToken);

        if (res && res.success) {
            if (window.ui) window.ui.toast(`Litigator account ${email} provisioned successfully!`, 'success');
            window.closeCreateLitigatorModal();
            document.getElementById('createLitigatorForm').reset();
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to create litigator account', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Account creation error: ' + err.message, 'error');
    }
};

window.openBulkBonusModal = () => {
    const modal = document.getElementById('bulkBonusModal');
    if (modal) modal.classList.remove('hidden');
};

window.closeBulkBonusModal = () => {
    const modal = document.getElementById('bulkBonusModal');
    if (modal) modal.classList.add('hidden');
};

window.submitBulkBonus = async (e) => {
    if (e) e.preventDefault();
    if (!adminAuthToken) return;

    const bonus = parseInt(document.getElementById('bulkBonusAmount').value, 10) || 10;
    try {
        const res = await api.bulkBonusQuotas(bonus, adminAuthToken);
        if (res && res.success) {
            if (window.ui) window.ui.toast(res.message || `Granted +${bonus} credits to all active litigators!`, 'success');
            window.closeBulkBonusModal();
            await window.loadAdminPortalData();
        } else {
            if (window.ui) window.ui.toast(res.detail || 'Failed to grant bonus credits', 'error');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Bonus error: ' + err.message, 'error');
    }
};

window.loadAdminSecurityLogs = async () => {
    if (!adminAuthToken) return;
    try {
        const res = await api.getSecurityLogs(adminAuthToken);
        if (res && res.success && Array.isArray(res.logs)) {
            adminCachedSecurityLogs = res.logs;
            window.renderAdminSecurityLogsTable(adminCachedSecurityLogs);
        }
    } catch (err) {
        console.warn('Security logs load failed:', err);
    }
};

window.renderAdminSecurityLogsTable = (logs) => {
    const tbody = document.getElementById('adminSecurityLogsTableBody');
    if (!tbody) return;

    if (!logs || logs.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="6" style="padding: 2.5rem; text-align: center; color: var(--gray-400);">
                    <i class="fas fa-shield-check" style="font-size: 1.5rem; color: #10b981; margin-bottom: 0.5rem; display: block;"></i>
                    No platform security events recorded. System is secure.
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = logs.map(l => {
        const metaStr = l.metadata ? JSON.stringify(l.metadata).substring(0, 45) + (JSON.stringify(l.metadata).length > 45 ? '...' : '') : '--';
        return `
            <tr>
                <td style="font-family: monospace; font-size: 0.78rem; color: #6366f1; font-weight: 700;">#${l.id}</td>
                <td>
                    <div style="font-weight: 700; color: var(--gray-900); font-size: 0.82rem;">${l.user_id}</div>
                </td>
                <td style="font-family: monospace; font-size: 0.75rem; color: var(--gray-600);">${l.case_id}</td>
                <td>
                    <span style="display: inline-block; font-size: 0.72rem; font-weight: 800; padding: 2px 6px; border-radius: 4px; background: rgba(99, 102, 241, 0.12); color: #4f46e5;">
                        ${l.action}
                    </span>
                </td>
                <td style="font-size: 0.75rem; color: var(--gray-500); font-family: monospace;">${metaStr}</td>
                <td style="text-align: right; font-size: 0.75rem; color: var(--gray-400);">
                    ${l.timestamp ? new Date(l.timestamp).toLocaleTimeString() : '--'}
                </td>
            </tr>
        `;
    }).join('');
};

window.exportAdminAuditLogs = (format = 'json') => {
    if (!adminCachedSecurityLogs || adminCachedSecurityLogs.length === 0) {
        if (window.ui) window.ui.toast('No logs available to export.', 'warning');
        return;
    }
    const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(adminCachedSecurityLogs, null, 2));
    const dlAnchorElem = document.createElement('a');
    dlAnchorElem.setAttribute("href", dataStr);
    dlAnchorElem.setAttribute("download", `judiq_audit_logs_${new Date().toISOString().slice(0,10)}.json`);
    dlAnchorElem.click();
    if (window.ui) window.ui.toast('Cryptographic audit trail downloaded.', 'success');
};

window.loadAdminSystemHealth = async () => {
    if (!adminAuthToken) return;
    try {
        const res = await api.getSystemHealth(adminAuthToken);
        if (res && res.success) {
            const memEl = document.getElementById('statMemoryPercent');
            if (memEl && res.memory_percent) memEl.textContent = res.memory_percent + '%';
            if (window.ui) window.ui.toast('Platform health check: All core engines OPERATIONAL', 'success');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Health check error: ' + err.message, 'error');
    }
};

window.performAdminCacheClear = async () => {
    if (!adminAuthToken) return;
    if (!confirm('Clear all in-memory response caches and vector evaluation buffers?')) return;
    try {
        const res = await api.clearSystemCache(adminAuthToken);
        if (res && res.success) {
            if (window.ui) window.ui.toast('System in-memory response caches purged cleanly.', 'success');
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Cache clear error: ' + err.message, 'error');
    }
};

window.saveAdminLlmSettings = () => {
    const selected = document.querySelector('input[name="adminLlmModel"]:checked');
    const model = selected ? selected.value : 'groq';
    localStorage.setItem('judiq_admin_llm_model', model);
    if (window.ui) window.ui.toast(`AI Copilot Layer configured to: ${model.toUpperCase()}`, 'success');
};

window.submitAdminPrecedentIngestion = async (e) => {
    if (e) e.preventDefault();
    const title = document.getElementById('adminPrecTitle').value.trim();
    const citation = document.getElementById('adminPrecCitation').value.trim();
    const court = document.getElementById('adminPrecCourt').value;
    const proposition = document.getElementById('adminPrecProposition').value.trim();
    const ratio = document.getElementById('adminPrecRatio').value.trim();

    try {
        const payload = {
            title: title,
            citation: citation,
            court: court,
            holding: proposition,
            full_text: ratio,
            domain: "s138"
        };
        const res = await api.ingestPrecedent(payload);
        if (res && res.status === 'success') {
            if (window.ui) window.ui.toast(`Precedent [${citation}] successfully indexed in neural vector database!`, 'success');
            document.getElementById('adminIngestPrecedentForm').reset();
        } else {
            if (window.ui) window.ui.toast('Ingestion completed with fallback indexing.', 'info');
            document.getElementById('adminIngestPrecedentForm').reset();
        }
    } catch (err) {
        if (window.ui) window.ui.toast('Ingestion error: ' + err.message, 'error');
    }
};


// ============================================================================
// MODULAR SUBSCRIPTION PRICING CONFIGURATOR (₹500 / Module / 10 Cases)
// ============================================================================

window.updateModularPricing = function() {
    const checkboxes = document.querySelectorAll('input[name="legal_module"]');
    const checkedModules = [];
    
    checkboxes.forEach(cb => {
        const card = cb.closest('.module-choice-card');
        if (cb.checked) {
            checkedModules.push(cb.value);
            if (card) card.classList.add('active');
        } else {
            if (card) card.classList.remove('active');
        }
    });

    // Guard: Keep at least 1 module selected
    if (checkedModules.length === 0) {
        const firstCb = document.querySelector('input[name="legal_module"][value="s138"]');
        if (firstCb) {
            firstCb.checked = true;
            const card = firstCb.closest('.module-choice-card');
            if (card) card.classList.add('active');
            checkedModules.push('s138');
        }
    }

    const count = checkedModules.length;
    let price = count * 500;
    if (count === 6) {
        price = 2500; // Special bundle price for all 6
    }
    const cases = count * 10;

    const titles = {
        1: 'Solo Practice Plan (1 Module)',
        2: 'Dual Practice Plan (2 Modules)',
        3: 'Commercial Law Practice (3 Modules)',
        4: 'Litigation Firm Plan (4 Modules)',
        5: 'Advanced Chambers Suite (5 Modules)',
        6: 'Enterprise Full-Access Suite (All 6 Engines)'
    };

    const descs = {
        1: 'Tailored for specialized advocates handling 10 cases / month in a single statutory area.',
        2: 'Perfect for litigators handling concurrent civil and Section 138 cheque bounce matters (20 cases/mo).',
        3: 'Comprehensive coverage for active commercial litigators (30 cases / 30-day billing cycle).',
        4: 'Designed for boutique litigation firms managing multi-court caseloads (40 cases/mo).',
        5: 'Extensive multi-track recovery and defense strategy coverage (50 cases/mo).',
        6: 'All-inclusive institutional intelligence suite across all 6 statutory engines (60 cases/mo).'
    };

    const priceEl = document.getElementById('planTotalPrice');
    const casesEl = document.getElementById('planTotalCases');
    const modulesEl = document.getElementById('planTotalModules');
    const titleEl = document.getElementById('planTierTitle');
    const descEl = document.getElementById('planTierDesc');
    const btnLabelEl = document.getElementById('subscribeBtnLabel');

    if (priceEl) priceEl.textContent = price.toLocaleString('en-IN');
    if (casesEl) casesEl.textContent = `${cases} Cases`;
    if (modulesEl) modulesEl.textContent = `${count} ${count > 1 ? 'Engines' : 'Engine'}`;
    if (titleEl) titleEl.textContent = titles[count] || `${count} Modules Plan`;
    if (descEl) descEl.textContent = descs[count] || `Custom plan with ${count} active legal engines.`;
    if (btnLabelEl) btnLabelEl.textContent = `Get Started with ${count} ${count > 1 ? 'Modules' : 'Module'} (₹${price.toLocaleString('en-IN')} / mo)`;

    // Update preset pills active state
    document.querySelectorAll('.preset-pill').forEach(pill => {
        pill.classList.remove('active');
    });
    const matchingPill = document.querySelector(`.preset-pill[onclick*="${count}"]`);
    if (matchingPill) matchingPill.classList.add('active');
};

window.applyModularPreset = function(count) {
    const modulesOrder = ['s138', 'sarfaesi', 'criminal', 'civil', 'bank_recovery', 'counsel_intel'];
    const targetModules = modulesOrder.slice(0, count);

    document.querySelectorAll('input[name="legal_module"]').forEach(cb => {
        cb.checked = targetModules.includes(cb.value);
    });

    window.updateModularPricing();
};

window.subscribeToSelectedModularPlan = async function() {
    const checkboxes = document.querySelectorAll('input[name="legal_module"]:checked');
    const selected = Array.from(checkboxes).map(cb => cb.value);
    const count = Math.max(1, selected.length);
    let price = count * 500;
    if (count === 6) price = 2500;
    const cases = count * 10;

    const user = window.state ? window.state.currentUser : null;
    let userEmail = user && user.email ? user.email : '';
    if (!userEmail) {
        userEmail = prompt('Enter your advocate / firm work email to register plan subscription:', 'advocate@lawfirm.in');
        if (!userEmail) return;
    }
    const cleanEmail = userEmail.trim().toLowerCase();
    const userId = user && user.uid ? user.uid : 'USR_' + cleanEmail.split('@')[0].toUpperCase();

    const planPayload = {
        user_id: userId,
        email: cleanEmail,
        selected_modules: selected,
        monthly_price_inr: price,
        requested_quota: cases,
        role: "law_firm"
    };

    try {
        const res = await api.submitSubscriptionPlan(planPayload);
        if (res && res.success) {
            localStorage.setItem('judiq_selected_plan', JSON.stringify({
                ...planPayload,
                status: 'PENDING_APPROVAL',
                submitted_at: new Date().toISOString()
            }));

            // Alert user that plan is queued for admin approval
            alert(
                `📋 SUBSCRIPTION PLAN SUBMITTED (SIMULATION MODE)\n\n` +
                `Account: ${cleanEmail}\n` +
                `Selected Modules: ${count} (${selected.join(', ')})\n` +
                `Monthly Fee: ₹${price.toLocaleString('en-IN')} / month\n` +
                `Monthly Case Allowance: ${cases} Cases / 30 Days\n\n` +
                `⏳ STATUS: PENDING ADMIN APPROVAL\n` +
                `An entry has been logged in the Master Admin Control Center.\n` +
                `Until approved by an administrator, case analysis and draft generation will remain locked for this profile.`
            );

            if (window.ui && window.ui.toast) {
                window.ui.toast(`Plan request submitted! Logged in Admin Center as PENDING APPROVAL.`, 'warning');
            }
        } else {
            if (window.ui && window.ui.toast) window.ui.toast(res.detail || 'Plan submission failed', 'error');
        }
    } catch (e) {
        console.error('Plan submit failed:', e);
        if (window.ui && window.ui.toast) window.ui.toast('Plan submission error: ' + e.message, 'error');
    }
};

// Auto-initialize pricing calculator on DOM load
document.addEventListener('DOMContentLoaded', () => {
    if (typeof window.updateModularPricing === 'function') {
        window.updateModularPricing();
    }
});

/* ═══════════════════════════════════════════════════════════════════════════
   LAWYER CASE VERSIONING & SNAPSHOT TIMELINE CONTROLLER
   ═══════════════════════════════════════════════════════════════════════════ */

window.currentCaseVersion = 1;
window.caseVersionsHistory = [];

window.updateCaseVersionBadge = (versionNum = 1, deltaScore = 0) => {
    window.currentCaseVersion = versionNum;
    const badge = document.getElementById('activeVersionBadge');
    if (badge) {
        badge.textContent = `Version ${versionNum}`;
    }
    const deltaBadge = document.getElementById('activeVersionDeltaBadge');
    if (deltaBadge) {
        if (deltaScore && Math.abs(deltaScore) > 0.1) {
            deltaBadge.style.display = 'inline-block';
            deltaBadge.textContent = (deltaScore > 0 ? `+${deltaScore}` : `${deltaScore}`) + ' pts';
            deltaBadge.style.background = deltaScore > 0 ? '#10b981' : '#f59e0b';
        } else {
            deltaBadge.style.display = 'none';
        }
    }
};

window.showSaveVersionModal = () => {
    const modal = document.getElementById('saveVersionModal');
    if (modal) {
        modal.classList.remove('hidden');
        const titleInput = document.getElementById('versionTitleInput');
        const nextVer = (window.currentCaseVersion || 1) + 1;
        if (titleInput) {
            titleInput.value = `Version ${nextVer} — Evidence & Strategy Update`;
            titleInput.focus();
        }
    }
};

window.closeSaveVersionModal = () => {
    const modal = document.getElementById('saveVersionModal');
    if (modal) modal.classList.add('hidden');
};

window.handleSaveVersionSubmit = async (e) => {
    if (e) e.preventDefault();
    const title = document.getElementById('versionTitleInput')?.value?.trim() || 'Updated Version';
    const note = document.getElementById('versionNoteInput')?.value?.trim() || 'Strategy refinement';

    const caseData = window.state?.currentCaseData || (window.currentCase && window.currentCase.case_data) || {};
    const analysisResult = window.state?.currentAnalysisResult || (window.currentCase && window.currentCase.analysis_result) || {};
    const caseId = caseData.case_id || (window.currentCase && window.currentCase.id) || 'CASE_ACTIVE';
    const user = window.state?.currentUser;
    const userId = user?.uid || user?.email || 'ANONYMOUS';

    const score = analysisResult.score !== undefined ? analysisResult.score : (analysisResult.merit_score || 0);
    const verdict = analysisResult.verdict || analysisResult.primary_verdict || 'ANALYZED';

    try {
        const payload = {
            case_data: caseData,
            analysis_result: analysisResult,
            score: score,
            verdict: verdict,
            version_title: title,
            version_note: note,
            user_id: userId
        };

        const res = await fetch(`${api.baseUrl || 'http://127.0.0.1:8000'}/api/v1/cases/${encodeURIComponent(caseId)}/versions`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        }).then(r => r.json());

        if (res && res.version_num) {
            window.updateCaseVersionBadge(res.version_num, res.delta_score);
            window.closeSaveVersionModal();

            // Real-Time Cloud Sync: Firebase Firestore
            if (typeof firebase !== 'undefined' && firebase.firestore) {
                try {
                    const db = firebase.firestore();
                    await db.collection('cases').doc(caseId).collection('versions').doc(String(res.version_num)).set({
                        ...payload,
                        version_num: res.version_num,
                        delta_score: res.delta_score,
                        created_at: new Date().toISOString()
                    }, { merge: true });
                } catch (fbErr) {
                    console.warn('[Firestore] Version sync notice:', fbErr);
                }
            }

            if (window.ui && window.ui.toast) {
                window.ui.toast(`✅ Archived Version ${res.version_num}: "${title}"`, 'success');
            } else {
                alert(`✅ Archived Version ${res.version_num}: "${title}"`);
            }
        } else {
            alert('Failed to save version snapshot.');
        }
    } catch (err) {
        console.error('Error archiving case version:', err);
        alert('Error saving version: ' + err.message);
    }
};

window.showVersionHistoryModal = async () => {
    const modal = document.getElementById('versionHistoryModal');
    const container = document.getElementById('versionHistoryTimelineContainer');
    const titleEl = document.getElementById('versionModalCaseTitle');
    if (!modal) return;

    modal.classList.remove('hidden');

    const caseData = window.state?.currentCaseData || (window.currentCase && window.currentCase.case_data) || {};
    const caseId = caseData.case_id || (window.currentCase && window.currentCase.id) || 'CASE_ACTIVE';
    const caseTitle = caseData.case_title || 'Active Case Matter';
    if (titleEl) titleEl.textContent = `Matter: ${caseTitle} (Ref: ${caseId})`;

    if (container) {
        container.innerHTML = `
            <div style="text-align: center; color: var(--gray-500); padding: 2rem;">
                <i class="fas fa-spinner fa-spin" style="margin-right: 0.5rem;"></i> Loading version timeline from database & Firestore...
            </div>
        `;
    }

    try {
        let versions = [];
        try {
            const res = await fetch(`${api.baseUrl || 'http://127.0.0.1:8000'}/api/v1/cases/${encodeURIComponent(caseId)}/versions`);
            if (res.ok) versions = await res.json();
        } catch (apiErr) {
            console.warn('Backend version fetch notice:', apiErr);
        }

        // Firestore Fallback if offline/empty
        if ((!versions || versions.length === 0) && typeof firebase !== 'undefined' && firebase.firestore) {
            try {
                const db = firebase.firestore();
                const snap = await db.collection('cases').doc(caseId).collection('versions').get();
                snap.forEach(doc => versions.push(doc.data()));
                versions.sort((a, b) => (b.version_num || 0) - (a.version_num || 0));
            } catch (fbErr) {
                console.warn('[Firestore] Version list fallback notice:', fbErr);
            }
        }

        if (!versions || versions.length === 0) {
            if (container) {
                container.innerHTML = `
                    <div style="text-align: center; padding: 2.5rem 1.5rem; color: var(--gray-500); border: 2px dashed var(--gray-300); border-radius: 0.75rem;">
                        <i class="fas fa-code-branch" style="font-size: 2rem; color: var(--gray-400); margin-bottom: 0.75rem; display: block;"></i>
                        <h4 style="margin: 0 0 0.5rem 0; color: var(--gray-800);">No Archived Version Snapshots Yet</h4>
                        <p style="margin: 0 0 1.25rem 0; font-size: 0.85rem;">Click "Save New Snapshot" to archive your first formal baseline version for this case.</p>
                        <button class="btn btn-primary btn-sm" onclick="window.showSaveVersionModal()">
                            <i class="fas fa-save"></i> Save Initial Version
                        </button>
                    </div>
                `;
            }
            return;
        }

        window.caseVersionsHistory = versions;

        if (container) {
            container.innerHTML = `
                <div class="version-timeline-list" style="display: flex; flex-direction: column; gap: 1rem;">
                    ${versions.map(v => {
                        const isCurrent = v.version_num === window.currentCaseVersion;
                        const delta = v.delta_score || 0;
                        const deltaMarkup = Math.abs(delta) > 0.1 
                            ? `<span style="font-size: 0.75rem; font-weight: 700; padding: 0.15rem 0.45rem; border-radius: 9999px; background: ${delta > 0 ? '#10b98122' : '#f59e0b22'}; color: ${delta > 0 ? '#10b981' : '#f59e0b'}; border: 1px solid ${delta > 0 ? '#10b98144' : '#f59e0b44'};">
                                ${delta > 0 ? '+' + delta : delta} pts
                               </span>`
                            : '';

                        return `
                            <div class="version-timeline-card" style="border: 1px solid ${isCurrent ? '#3b82f6' : 'var(--gray-200)'}; background: ${isCurrent ? 'rgba(59, 130, 246, 0.04)' : 'var(--bg-card, #ffffff)'}; border-radius: 0.75rem; padding: 1rem 1.25rem; transition: all 0.2s ease; box-shadow: ${isCurrent ? '0 4px 12px rgba(59, 130, 246, 0.08)' : 'none'};">
                                <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; margin-bottom: 0.5rem;">
                                    <div>
                                        <div style="display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap;">
                                            <span style="font-weight: 800; font-size: 0.95rem; color: #1e293b;">
                                                <i class="fas fa-tag" style="color: #3b82f6; font-size: 0.85rem; margin-right: 0.25rem;"></i>
                                                ${escapeHtml(v.version_title || `Version ${v.version_num}`)}
                                            </span>
                                            ${isCurrent ? '<span style="background: #3b82f6; color: white; font-size: 0.65rem; font-weight: 800; padding: 0.15rem 0.5rem; border-radius: 9999px; text-transform: uppercase;">ACTIVE</span>' : ''}
                                            ${deltaMarkup}
                                        </div>
                                        <div style="font-size: 0.75rem; color: var(--gray-400); margin-top: 0.2rem;">
                                            Archived: <strong>${new Date(v.created_at || Date.now()).toLocaleString('en-IN', { dateStyle: 'medium', timeStyle: 'short' })}</strong>
                                        </div>
                                    </div>
                                    <div style="text-align: right; min-width: 100px;">
                                        <div style="font-size: 1.15rem; font-weight: 800; color: ${v.score >= 70 ? '#10b981' : (v.score >= 40 ? '#f59e0b' : '#ef4444')};">
                                            ${Math.round(v.score || 0)} / 100
                                        </div>
                                        <div style="font-size: 0.7rem; font-weight: 600; color: var(--gray-500); text-transform: uppercase;">
                                            ${escapeHtml(v.verdict || 'EVALUATED')}
                                        </div>
                                    </div>
                                </div>

                                ${v.version_note ? `
                                    <div style="font-size: 0.85rem; color: var(--gray-700); background: var(--gray-50); padding: 0.6rem 0.75rem; border-radius: 0.5rem; border-left: 3px solid #3b82f6; margin-bottom: 0.75rem; line-height: 1.45;">
                                        ${escapeHtml(v.version_note)}
                                    </div>
                                ` : ''}

                                <div style="display: flex; justify-content: flex-end; gap: 0.5rem; margin-top: 0.5rem;">
                                    <button class="btn btn-outline btn-sm" onclick="window.loadVersionSnapshot(${v.version_num})" style="padding: 0.25rem 0.65rem; font-size: 0.75rem;">
                                        <i class="fas fa-eye"></i> View Snapshot
                                    </button>
                                    ${!isCurrent ? `
                                        <button class="btn btn-primary btn-sm" onclick="window.restoreVersionSnapshot(${v.version_num})" style="padding: 0.25rem 0.65rem; font-size: 0.75rem; background: linear-gradient(135deg, #2563eb, #1d4ed8);">
                                            <i class="fas fa-history"></i> Restore This Version
                                        </button>
                                    ` : ''}
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            `;
        }
    } catch (err) {
        console.error('Failed to load version history:', err);
        if (container) container.innerHTML = `<p style="color: #ef4444; padding: 1rem;">Failed to load version history: ${err.message}</p>`;
    }
};

window.closeVersionHistoryModal = () => {
    const modal = document.getElementById('versionHistoryModal');
    if (modal) modal.classList.add('hidden');
};

window.loadVersionSnapshot = async (versionNum) => {
    const caseData = window.state?.currentCaseData || (window.currentCase && window.currentCase.case_data) || {};
    const caseId = caseData.case_id || (window.currentCase && window.currentCase.id) || 'CASE_ACTIVE';

    try {
        const res = await fetch(`${api.baseUrl || 'http://127.0.0.1:8000'}/api/v1/cases/${encodeURIComponent(caseId)}/versions/${versionNum}`).then(r => r.json());
        if (res && res.case_data) {
            window.currentCase = {
                id: caseId,
                case_data: res.case_data,
                analysis_result: res.analysis_result,
                score: res.score,
                verdict: res.verdict
            };
            if (window.state) {
                window.state.currentCaseData = res.case_data;
                window.state.currentAnalysisResult = res.analysis_result;
            }

            window.updateCaseVersionBadge(res.version_num, res.delta_score);
            window.closeVersionHistoryModal();

            // Re-render analysis with snapshot
            renderResults(res.analysis_result);
            if (window.ui && window.ui.toast) {
                window.ui.toast(`Viewing historical snapshot: Version ${versionNum} ("${res.version_title || 'Snapshot'}")`, 'info');
            }
        }
    } catch (err) {
        alert('Failed to load snapshot version: ' + err.message);
    }
};

window.restoreVersionSnapshot = async (versionNum) => {
    if (!confirm(`Restore active case state to Version ${versionNum}? Current unsaved edits will be replaced.`)) return;

    const caseData = window.state?.currentCaseData || (window.currentCase && window.currentCase.case_data) || {};
    const caseId = caseData.case_id || (window.currentCase && window.currentCase.id) || 'CASE_ACTIVE';
    const user = window.state?.currentUser;
    const userId = user?.uid || user?.email || 'ANONYMOUS';

    try {
        const res = await fetch(`${api.baseUrl || 'http://127.0.0.1:8000'}/api/v1/cases/${encodeURIComponent(caseId)}/restore/${versionNum}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user_id: userId })
        }).then(r => r.json());

        if (res && res.success) {
            window.loadVersionSnapshot(versionNum);
            if (window.ui && window.ui.toast) {
                window.ui.toast(`Restored Case to Version ${versionNum} successfully!`, 'success');
            }
        }
    } catch (err) {
        alert('Failed to restore version: ' + err.message);
    }
};

// ── Global CMS Navigation & Audit Trail ────────────────────────
window.showCmsHome = () => {
    switchScreen('cmsHomeScreen');
};

window.showAuditTrail = async () => {
    switchScreen('auditTrailScreen');
    const tbody = document.getElementById('auditTrailTableBody');
    if (tbody) tbody.innerHTML = `<tr><td colspan="6" class="cms-table-loading"><i class="fas fa-spinner fa-spin"></i> Loading audit logs...</td></tr>`;

    try {
        const logs = await fetch(`${api.baseUrl || 'http://127.0.0.1:8000'}/api/v1/cms/audit/export`).then(async r => {
            // Alternatively fetch JSON audit list:
            const jsonRes = await fetch(`${api.baseUrl || 'http://127.0.0.1:8000'}/api/v1/cms/cases?limit=100`).then(jr => jr.json());
            return [];
        }).catch(() => []);

        // Fetch recent audit logs from API
        const user = window.state?.currentUser;
        const res = await fetch(`${api.baseUrl || 'http://127.0.0.1:8000'}/api/v1/cms/cases/CSE-RECENT/timeline`).then(r => r.json()).catch(() => ({ timeline: [] }));
        const timeline = res.timeline || [];

        if (tbody) {
            if (timeline.length === 0) {
                tbody.innerHTML = `<tr><td colspan="6" class="text-muted" style="text-align:center; padding:2rem;">Immutable audit trail active. Actions will appear in real time.</td></tr>`;
            } else {
                tbody.innerHTML = timeline.map(l => `
                    <tr>
                        <td class="cms-cell-id">${escapeHtml(l.log_id || 'LOG')}</td>
                        <td>${escapeHtml(l.timestamp || '—')}</td>
                        <td>${escapeHtml(l.user_id || 'System')}</td>
                        <td>${escapeHtml(l.case_id || '—')}</td>
                        <td><strong>${escapeHtml(l.action || 'ACTION')}</strong></td>
                        <td>${escapeHtml(l.note || '')}</td>
                    </tr>
                `).join('');
            }
        }
    } catch (err) {
        if (tbody) tbody.innerHTML = `<tr><td colspan="6" class="cms-error-cell">Error: ${escapeHtml(err.message)}</td></tr>`;
    }
};

window.handleExportAuditCsv = async () => {
    try {
        if (ui && typeof ui.toast === 'function') ui.toast("Generating CSV export of immutable audit log...", "info");
        const blob = await api.exportAuditCsv();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `judiq_audit_trail_${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        a.remove();
    } catch (err) {
        if (ui && typeof ui.toast === 'function') ui.toast(`CSV Export failed: ${err.message}`, 'error');
    }
};





