/**
 * JudiQ AI — Internationalization (i18n) Engine for English, Marathi (मराठी), Hindi (हिंदी) & Gujarati (ગુજરાતી)
 * Enables seamless instant switching across landing page, input forms, dashboard, results, and draft generation.
 */

window.i18n = {
    currentLang: localStorage.getItem('judiq_lang') || 'en',

    dictionary: {
        en: {
            brand_name: "JUDIQ AI",
            brand_subtitle: "AI Litigation Strategist",
            nav_features: "Features",
            nav_simulator: "Simulator",
            nav_readiness: "Readiness",
            nav_pricing: "Pricing",
            nav_about: "About",
            nav_testimonials: "Testimonials",
            nav_faq: "FAQ",
            nav_contact: "Contact",
            nav_docs: "Docs",
            nav_settings: "Settings",
            nav_logout: "Logout",
            load_demo_case: "Load Demo Case",
            hero_title: "Find Fatal Legal Defects Before The Courtroom Does",
            hero_sub: "Adversarial litigation intelligence, Section 138 cheque bounce audit, statutory limitation computing, and automated legal draft generator.",
            start_analysis: "Start Case Analysis",
            select_domain: "Select Legal Domain",
            domain_ni: "Cheque Bounce (Section 138 NI Act)",
            domain_sarfaesi: "SARFAESI & DRT Litigation",
            domain_criminal: "Criminal Law (IPC / BNS)",
            domain_civil: "Civil Suit (CPC)",
            account_overview: "Account Overview",
            quick_actions: "Quick Actions",
            cases_analysed: "Cases Analysed",
            fatal_defects: "Fatal Defects Found",
            strong_cases: "Strong Cases",
            analyse_s138: "Analyse S.138 Case",
            s138_sub: "Run adversarial weakness scan — Limitation, Notice, Instrument, Debt",
            generate_draft: "Generate Legal Draft",
            generate_draft_sub: "Auto-generate demand notice, S.138 complaint, or defence reply",
            cheque_amount: "Cheque Amount (₹)",
            dishonour_date: "Date of Dishonour Memo",
            notice_date: "Date of Statutory Demand Notice",
            complainant_type: "Complainant Entity Type",
            analyze_now: "Analyze Case Now",
            viability_score: "Litigation Viability Score",
            procedural_timeline: "Procedural Milestone Graph",
            next_best_actions: "Recommended Next Actions",
            adversarial_vectors: "Adversarial Attack Vectors & Weaknesses",
            export_pdf: "Export Analysis PDF",
            lang_label: "English",
            sim_heading: "Interactive Cross-Examination Risk Simulator",
            sim_sub: "Test litigation strategies live, simulate opposing counsel attack vectors, and calculate real-time courtroom survivability.",
            counsel_dock_title: "JudIQ Co-Counsel Dock",
            counsel_dock_prompt1: "Bank Manager Cross-Exam",
            counsel_dock_prompt2: "S.138 Limitation Rules",
            counsel_dock_prompt3: "Security Cheque Ratios",
            counsel_dock_prompt4: "Section 313 Defense"
        },
        mr: {
            brand_name: "ज्युरिक आय (JudiQ AI)",
            brand_subtitle: "न्यायालयीन AI रणनीतिकार",
            nav_features: "वैशिष्ट्ये",
            nav_simulator: "सिम्युलेटर",
            nav_readiness: "तयारी",
            nav_pricing: "किंमत (प्लॅन्स)",
            nav_about: "माहिती",
            nav_testimonials: "प्रतिक्रिया",
            nav_faq: "प्रश्न",
            nav_contact: "संपर्क",
            nav_docs: "दस्तऐवज",
            nav_settings: "सेटिंग्ज",
            nav_logout: "लॉगआउट",
            load_demo_case: "डेमो प्रकरण लोड करा",
            hero_title: "न्यायालयात जाण्यापूर्वी कायदेशीर त्रुटी आणि कमकुवत बाबी शोधा",
            hero_sub: "कलम १३८ चेक बाऊन्स विश्लेषण, सरफेसी बँक कारवाई, विरोधी युक्तिवाद चाचणी आणि स्वयंचलित कायदेशीर मसुदा.",
            start_analysis: "प्रकरणाचे विश्लेषण सुरू करा",
            select_domain: "कायदेशीर क्षेत्र निवडा",
            domain_ni: "चेक बाऊन्स (कलम १३८ एन.आय. ॲक्ट)",
            domain_sarfaesi: "सरफेसी आणि डी.आर.टी. कायदा",
            domain_criminal: "फौजदारी कायदा (आयपीसी / बीएनएस)",
            domain_civil: "दीवाणी दावा (सीपीसी)",
            account_overview: "खाते विहंगावलोकन",
            quick_actions: "जलद कृती पर्याय",
            cases_analysed: "विश्लेषण केलेली प्रकरणे",
            fatal_defects: "गंभीर कायदेशीर त्रुटी",
            strong_cases: "भक्कम प्रकरणे",
            analyse_s138: "कलम १३८ प्रकरणाचे विश्लेषण",
            s138_sub: "कायदेशीर नोटीस, मुदत, रक्कम व नोटीस त्रुटींची पडताळणी करा",
            generate_draft: "कायदेशीर मसुदा तयार करा",
            generate_draft_sub: "मागणी नोटीस, तक्रार अर्ज किंवा बचाव उत्तर स्वयंचलित तयार करा",
            cheque_amount: "चेकची रक्कम (₹)",
            dishonour_date: "चेक अनादर (बाऊन्स) तारीख",
            notice_date: "कायदेशीर नोटीस पाठवल्याची तारीख",
            complainant_type: "तक्रारदाराचा प्रकार",
            analyze_now: "आत्ता विश्लेषण करा",
            viability_score: "खटल्याची संभाव्य यश टक्केवारी",
            procedural_timeline: "प्रक्रियात्मक कालमर्यादा आलेख",
            next_best_actions: "महत्त्वाच्या पुढील कृती",
            adversarial_vectors: "विरोधी पक्षाचे संभाव्य युक्तिवाद आणि त्रुटी",
            export_pdf: "विश्लेषण पीडीएफ डाऊनलोड करा",
            lang_label: "मराठी",
            sim_heading: "उलटतपासणी धोका सिम्युलेटर",
            sim_sub: "न्यायालयीन रणनीतींची थेट चाचणी करा आणि संभाव्य यशाची टक्केवारी मोजा.",
            counsel_dock_title: "ज्युरिक एआय सह-सल्लागार",
            counsel_dock_prompt1: "बँक व्यवस्थापक उलटतपासणी",
            counsel_dock_prompt2: "कलम १३८ मुदत नियम",
            counsel_dock_prompt3: "सुरक्षा चेक न्यायनिवाडा",
            counsel_dock_prompt4: "कलम ३१३ बचाव उत्तर"
        },
        hi: {
            brand_name: "न्यायिक एआई (JudiQ AI)",
            brand_subtitle: "न्यायालीन एआई रणनीतिकार",
            nav_features: "विशेषताएं",
            nav_simulator: "सिम्युलेटर",
            nav_readiness: "तैयारी",
            nav_pricing: "शुल्क (प्लान्स)",
            nav_about: "परिचय",
            nav_testimonials: "समीक्षाएं",
            nav_faq: "सामान्य प्रश्न",
            nav_contact: "संपर्क",
            nav_docs: "दस्तावेज़",
            nav_settings: "सेटिंग्स",
            nav_logout: "लॉगआउट",
            load_demo_case: "डेमो केस लोड करें",
            hero_title: "अदालत में सुनवाई से पहले गंभीर कानूनी कमियां और त्रुटियां खोजें",
            hero_sub: "धारा 138 चेक बाउंस विश्लेषण, सरफेसी बैंक कार्रवाई, विपक्षी तर्क परीक्षण और स्वचालित कानूनी प्रारूप निर्माता।",
            start_analysis: "केस विश्लेषण शुरू करें",
            select_domain: "कानूनी क्षेत्र चुनें",
            domain_ni: "चेक बाउंस (धारा 138 एनआई एक्ट)",
            domain_sarfaesi: "सरफेसी एवं डीआरटी कानून",
            domain_criminal: "आपराधिक कानून (आईपीसी / बीएनएस)",
            domain_civil: "दीवानी मुकदमा (सीपीसी)",
            account_overview: "खाता विहंगम दृष्टि",
            quick_actions: "त्वरित कार्रवाई",
            cases_analysed: "विश्लेषित मुकदमे",
            fatal_defects: "गंभीर कानूनी कमियां",
            strong_cases: "मजबूत मुकदमे",
            analyse_s138: "धारा 138 केस विश्लेषण",
            s138_sub: "वैधानिक नोटिस, समय-सीमा, चेक राशि और नोटिस कमियों की जांच करें",
            generate_draft: "कानूनी ड्राफ्ट तैयार करें",
            generate_draft_sub: "मांग नोटिस, परिवाद पत्र या बचाव जवाब स्वचालित रूप से बनाएं",
            cheque_amount: "चेक राशि (₹)",
            dishonour_date: "चेक अनादर (बाउंस) तिथि",
            notice_date: "कानूनी नोटिस भेजने की तिथि",
            complainant_type: "शिकायतकर्ता का प्रकार",
            analyze_now: "अभी विश्लेषण करें",
            viability_score: "मुकदमे की सफलता की संभावना (%)",
            procedural_timeline: "प्रक्रियात्मक समय-सीमा आरेख",
            next_best_actions: "अनुशंसित आगामी कार्रवाई",
            adversarial_vectors: "विपक्षी पक्ष के संभावित तर्क और कमियां",
            export_pdf: "विश्लेषण पीडीएफ डाउनलोड करें",
            lang_label: "हिंदी",
            sim_heading: "जिरह जोखिम सिम्युलेटर",
            sim_sub: "अदालती रणनीतियों का सीधा परीक्षण करें और सफलता की संभावना का आकलन करें।",
            counsel_dock_title: "न्यायिक एआई सह-सलाहकार",
            counsel_dock_prompt1: "बैंक प्रबंधक जिरह",
            counsel_dock_prompt2: "धारा 138 समय-सीमा नियम",
            counsel_dock_prompt3: "सिक्योरिटी चेक कानून",
            counsel_dock_prompt4: "धारा 313 बचाव जवाब"
        },
        gu: {
            brand_name: "જ્યુડિક એઆઈ (JudiQ AI)",
            brand_subtitle: "ન્યાયાલયીન એઆઈ વ્યૂહરચનાકાર",
            nav_features: "વિશેષતાઓ",
            nav_simulator: "સિમ્યુલેટર",
            nav_readiness: "સજ્જતા",
            nav_pricing: "કિંમત (પ્લાન)",
            nav_about: "પરિચય",
            nav_testimonials: "અભિપ્રાય",
            nav_faq: "પ્રશ્નો",
            nav_contact: "સંપર્ક",
            nav_docs: "દસ્તાવેજ",
            nav_settings: "સેટિંગ્સ",
            nav_logout: "લોગઆઉટ",
            load_demo_case: "ડેમો કેસ લોડ કરો",
            hero_title: "અદાલતમાં જતા પહેલા ગંભીર કાનૂની ખામીઓ અને ક્ષતિઓ શોધો",
            hero_sub: "કલમ ૧૩૮ ચેક બાઉન્સ વિશ્લેષણ, સરફેસી બેંક કાર્યવાહી, વિરોધી દલીલ ચકાસણી અને સ્વચાલિત કાનૂની મુસદ્દો.",
            start_analysis: "કેસ વિશ્લેષણ શરૂ કરો",
            select_domain: "કાનૂની ક્ષેત્ર પસંદ કરો",
            domain_ni: "ચેક બાઉન્સ (કલમ ૧૩૮ એન.આઈ. એક્ટ)",
            domain_sarfaesi: "સરફેસી અને ડી.આર.ટી. કાયદો",
            domain_criminal: "ફોજદારી કાયદો (આઈપીસી / બીએનએસ)",
            domain_civil: "દીવાની દાવો (સીપીસી)",
            account_overview: "ખાતાની ઝાંખી",
            quick_actions: "ઝડપી ક્રિયાઓ",
            cases_analysed: "વિશ્લેષિત કેસો",
            fatal_defects: "ગંભીર કાનૂની ક્ષતિઓ",
            strong_cases: "મજબૂત કેસો",
            analyse_s138: "કલમ ૧૩૮ કેસ વિશ્લેષણ",
            s138_sub: "કાનૂની નોટિસ, મુદત, રકમ અને નોટિસ ક્ષતિઓની ચકાસણી કરો",
            generate_draft: "કાનૂની ડ્રાફ્ટ તૈયાર કરો",
            generate_draft_sub: "ડિમાન્ડ નોટિસ, ફરિયાદ અથવા બચાવ જવાબ આપમેળે બનાવો",
            cheque_amount: "ચેકની રકમ (₹)",
            dishonour_date: "ચેક અસ્વીકાર (બાઉન્સ) તારીખ",
            notice_date: "કાનૂની નોટિસ મોકલ્યાની તારીખ",
            complainant_type: "ફરિયાદીનો પ્રકાર",
            analyze_now: "હવે વિશ્લેષણ કરો",
            viability_score: "કેસની સફળતાની સંભાવના (%)",
            procedural_timeline: "પ્રક્રિયાત્મક સમયરેખા આલેખ",
            next_best_actions: "ભલામણ કરેલ આગામી પગલાં",
            adversarial_vectors: "સામા પક્ષની સંભવિત દલીલો અને ખામીઓ",
            export_pdf: "વિશ્લેષણ પીડીએફ ડાઉનલોડ કરો",
            lang_label: "ગુજરાતી",
            sim_heading: "ઉલટતપાસ જોખમ સિમ્યુલેટર",
            sim_sub: "અદાલતી વ્યૂહરચનાઓનું સીધું પરીક્ષણ કરો અને સંભવિત સફળતાની ગણતરી કરો.",
            counsel_dock_title: "જ્યુડિક એઆઈ સહ-સલાહકાર",
            counsel_dock_prompt1: "બેંક મેનેજર ઉલટતપાસ",
            counsel_dock_prompt2: "કલમ ૧૩૮ મુદતના નિયમો",
            counsel_dock_prompt3: "સિક્યોરિટી ચેક ચુકાદાઓ",
            counsel_dock_prompt4: "કલમ ૩૧૩ બચાવ જવાબ"
        }
    },

    t: function(key) {
        const lang = this.currentLang;
        const dict = this.dictionary[lang] || this.dictionary['en'];
        return dict[key] || this.dictionary['en'][key] || key;
    },

    updatePageText: function() {
        const lang = this.currentLang;
        document.documentElement.lang = lang;

        // Update all elements with data-i18n attribute
        document.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.getAttribute('data-i18n');
            const txt = this.t(key);
            if (txt) {
                if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
                    el.placeholder = txt;
                } else {
                    el.textContent = txt;
                }
            }
        });

        // Update language toggle buttons
        const labelMap = { en: 'English', mr: 'मराठी', hi: 'हिंदी', gu: 'ગુજરાતી' };
        document.querySelectorAll('.lang-toggle-btn').forEach(btn => {
            btn.innerHTML = `<i class="fas fa-globe"></i> <span>${labelMap[lang] || 'English'}</span>`;
        });
    }
};

window.toggleLanguage = function() {
    const cycle = { en: 'mr', mr: 'hi', hi: 'gu', gu: 'en' };
    window.i18n.currentLang = cycle[window.i18n.currentLang] || 'en';
    localStorage.setItem('judiq_lang', window.i18n.currentLang);
    window.i18n.updatePageText();
    
    // Re-render active screens if available
    if (window.renderDashboard && document.getElementById('dashboardScreen') && !document.getElementById('dashboardScreen').classList.contains('hidden')) {
        window.renderDashboard();
    }
    
    const toastMsgMap = {
        mr: 'भाषा मराठीत बदलली आहे.',
        hi: 'भाषा बदलकर हिंदी कर दी गई है।',
        gu: 'ભાષા બદલીને ગુજરાતી કરવામાં આવી છે.',
        en: 'Language switched to English.'
    };

    if (window.showToast) {
        window.showToast(toastMsgMap[window.i18n.currentLang] || 'Language updated.', 'info');
    } else if (window.toast && window.toast.show) {
        window.toast.show(toastMsgMap[window.i18n.currentLang] || 'Language updated.', 'info');
    }
};

document.addEventListener('DOMContentLoaded', () => {
    window.i18n.updatePageText();
});
