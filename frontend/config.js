export const API_BASE_URL = window.__JUDIQ_ENV__?.API_BASE_URL || (
    window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
        ? `${window.location.protocol}//${window.location.host}`
        : (window.location.origin && window.location.origin !== "null" && !window.location.origin.startsWith("file://")
            ? window.location.origin
            : "https://cheque-bounce-ragbased.onrender.com")
);

// Firebase Client Configuration
// SECURITY NOTICE: In the Firebase Web SDK, these values are client-side project coordinates.
// Database and storage security MUST be strictly governed by Firebase Security Rules on Firestore.
// Production Firestore rules should ensure that users can only read/write their own case records.
export const firebaseConfig = window.__JUDIQ_ENV__?.firebaseConfig || {
    apiKey: "AIzaSyBdqc1C8LPVj4zqvWJWJWMrXhPad20MZCw",
    authDomain: "idcourt-cb58f.firebaseapp.com",
    projectId: "idcourt-cb58f",
    storageBucket: "idcourt-cb58f.firebasestorage.app",
    messagingSenderId: "941086914513",
    appId: "1:941086914513:web:8edad96b7e9f0dd4be12f0",
    measurementId: "G-YQMJ6KXGBR"
};

export const wizardSteps = [
    {
        id: 'case_identity',
        title: 'Case Identity',
        subtitle: 'Basic case information and filing details',
        fields: [
            { name: 'case_id', label: 'Case ID', type: 'text', required: false, placeholder: 'e.g., CC/2024/123' },
            { name: 'case_title', label: 'Case Title', type: 'text', required: true, placeholder: 'Complainant vs Accused' },
            { name: 'complainant_type', label: 'Complainant Entity Type', type: 'select', options: ['Individual', 'Partnership Firm', 'Pvt Ltd/Ltd Company', 'HUF', 'Proprietorship'], required: true },
            { name: 'filing_date', label: 'Filing Date', type: 'date', required: true },
            { name: 'court_name', label: 'Court Name', type: 'text', required: false, placeholder: 'e.g., District Court, Mumbai' },
            { name: 'condonation_attached', label: 'Condonation of Delay App Attached? (S.142)', type: 'select', options: ['Yes', 'No', 'Not Applicable'], required: false },
            { name: 'case_type', label: 'Case Type', type: 'select', options: ['Cheque Bounce (Section 138 NI Act)'], required: true },
            { name: 'judicial_temperament', label: 'Judicial Temperament / Courtroom Mood', type: 'select', options: ['Balanced', 'Pro-Complainant', 'Pro-Accused'], required: false }
        ]
    },
    {
        id: 'parties',
        title: 'Parties Information',
        subtitle: 'Details of complainant and accused',
        fields: [
            { name: 'complainant_name', label: 'Complainant Name', type: 'text', required: true },
            { name: 'complainant_address', label: 'Complainant Address', type: 'textarea', required: true },
            { name: 'complainant_authorized', label: 'Board Resolution/Authorization Available? (If Entity)', type: 'select', options: ['Yes - Original', 'Yes - Copy', 'No', 'Not Applicable'], required: true },
            { name: 'accused_name', label: 'Accused Name / Company Name', type: 'text', required: true },
            { name: 'accused_type', label: 'Accused Entity Type', type: 'select', options: ['Individual', 'Pvt Ltd/Ltd Company', 'Partnership Firm', 'LLP', 'Other'], required: true },
            { name: 'accused_address', label: 'Accused Address', type: 'textarea', required: true },
            { name: 'directors_named', label: 'Company Arraigned & Directors Named? (S.141)', type: 'select', options: ['Yes - Company as A1 and Directors/Partners Named', 'Yes - Partial', 'No - Only Directors Sued (Aneeta Hada Fatal Defect)', 'Not Applicable'], required: true },
            { name: 'director_role_category', label: 'Director / Accused Executive Role (S.M.S. Pharma Test)', type: 'select', options: ['Managing Director / Whole-Time Director (Inherent Liability)', 'Authorized Cheque Signatory (Direct Liability)', 'Executive Director (Active Operations Pled)', 'Non-Executive / Independent Director (Strict Averments Required)', 'Sleeping Partner / Nominee Director', 'Not Applicable (Individual Accused)'], required: false },
            { name: 'active_management_averment', label: 'Specific Day-to-Day Control Pled in Complaint? (S.141)', type: 'select', options: ['Yes - Expressly averred in charge of day-to-day business (S.M.S. Pharma Standard)', 'Yes - Named as Authorized Cheque Signatory', 'No - General / Omnibus Impleadment Only (High Quashing Risk)', 'Not Applicable'], required: false },
            { name: 'accused_directors', label: 'Names of Directors/Partners Responsible', type: 'textarea', required: false, placeholder: 'e.g., Mr. A (Managing Director), Mr. B (Director)' }
        ]
    },
    {
        id: 'transaction',
        title: 'Transaction Details',
        subtitle: 'Underlying debt and transaction information',
        fields: [
            { name: 'transaction_date', label: 'Transaction Date', type: 'date', required: true },
            { name: 'purpose', label: 'Purpose of Transaction', type: 'textarea', required: true, placeholder: 'Describe the reason for the debt/loan...' },
            { name: 'agreement_type', label: 'Agreement Type', type: 'select', options: ['Written Agreement', 'Verbal Agreement', 'Invoice/Bill', 'Promissory Note', 'No Formal Agreement'], required: true },
            { name: 'itr_available', label: 'Complainant ITR Available? (Financial Capacity)', type: 'select', options: ['Yes', 'No'], required: false },
            { name: 'loan_advanced_via', label: 'How was the loan/debt advanced?', type: 'select', options: ['Bank Transfer (NEFT/RTGS/IMPS)', 'Account Payee Cheque', 'Cash', 'Other (e.g., Invoices/Goods)'], required: true }
        ]
    },
    {
        id: 'cheque',
        title: 'Cheque Details',
        subtitle: 'Information about the dishonoured cheque',
        fields: [
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true, placeholder: '123456' },
            { name: 'cheque_date', label: 'Cheque Date', type: 'date', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (₹)', type: 'number', required: true },
            { name: 'bank_name', label: 'Bank Name', type: 'text', required: true },
            { name: 'branch_name', label: 'Branch Name', type: 'text', required: false },
            { name: 'account_number', label: 'Account Number', type: 'text', required: false },
            { name: 'cheque_type', label: 'Cheque Type', type: 'select', options: ['Bearer Cheque', 'Account Payee Cheque', 'Crossed Cheque'], required: true },
            { name: 'post_dated', label: 'Post-Dated Cheque?', type: 'select', options: ['Yes', 'No'], required: true }
        ]
    },
    {
        id: 'dishonour',
        title: 'Dishonour Information',
        subtitle: 'Details of cheque dishonour and bank memo',
        fields: [
            { name: 'dishonour_date', label: 'Dishonour Date', type: 'date', required: true },
            { name: 'dishonour_reason', label: 'Reason for Dishonour', type: 'select', options: ['Insufficient Funds', 'Funds Insufficient', 'Account Closed', 'Signature Mismatch', 'Signature Differs', 'Payment Stopped', 'Refer to Drawer', 'Other'], required: true },
            { name: 'bank_memo_received', label: 'Bank Dishonour Memo Received?', type: 'select', options: ['Yes', 'No'], required: true },
            { name: 'memo_date', label: 'Memo Received Date', type: 'date', required: false },
            { name: 'memo_signed', label: 'Is Bank Memo Signed & Stamped?', type: 'select', options: ['Yes - Signed & Stamped', 'No - Unsigned/Digital', 'Unsure'], required: false },
            { name: 'presentation_date', label: 'First Presentation Date', type: 'date', required: true },
            { name: 'second_presentation', label: 'Second Presentation Made?', type: 'select', options: ['Yes', 'No', 'Not Applicable'], required: false },
            { name: 'second_presentation_date', label: 'Second Presentation Date', type: 'date', required: false }
        ]
    },
    {
        id: 'notice',
        title: 'Legal Notice (Section 138)',
        subtitle: 'Statutory notice details under NI Act',
        fields: [
            { name: 'notice_sent', label: 'Legal Notice Sent?', type: 'select', options: ['Yes', 'Yes - Being Sent', 'No'], required: true },
            { name: 'notice_date', label: 'Notice Sent Date', type: 'date', required: false },
            { name: 'notice_mode', label: 'Mode of Sending Notice', type: 'select', options: ['Registered Post AD', 'Speed Post', 'Courier', 'Email (Not Recommended)', 'Hand Delivery', 'Multiple Modes'], required: false },
            { name: 'notice_received', label: 'Notice Received by Accused?', type: 'select', options: ['Yes - Acknowledged', 'Yes - Refused', 'Returned Unserved', 'Unknown'], required: false },
            { name: 'notice_received_date', label: 'Notice Received/Refused Date', type: 'date', required: false },
            { name: 'reply_received', label: 'Reply from Accused Received?', type: 'select', options: ['Yes - Full Payment', 'Yes - Denial', 'Yes - Partial Response', 'No Reply'], required: false }
        ]
    },
    {
        id: 'evidence',
        title: 'Evidence & Documentation',
        subtitle: 'Available evidence to support your case',
        fields: [
            { name: 'original_cheque', label: 'Original Cheque Available?', type: 'select', options: ['Yes - Original', 'No - Lost', 'No - With Bank'], required: true },
            { name: 'agreement_documents', label: 'Loan/Agreement Documents?', type: 'select', options: ['Yes - Signed Agreement', 'Yes - Unsigned Draft', 'Promissory Note', 'None'], required: false },
            { name: 'witness_available', label: 'Witnesses Available?', type: 'select', options: ['Yes - Multiple', 'Yes - One', 'No'], required: false },
            { name: 'communication_records', label: 'Email/SMS/WhatsApp Records?', type: 'select', options: ['Yes - Extensive', 'Yes - Limited', 'No'], required: false },
            { name: 'has_bsa_certificate', label: 'S.63(4) BSA Certificate Attached?', type: 'select', options: ['Yes - Signed Certificate', 'No', 'Not Applicable'], required: false },
            { name: 'bank_statements', label: 'Bank Statements Available?', type: 'select', options: ['Yes - Complete', 'Yes - Partial', 'No'], required: false },
            { name: 'receipts_invoices', label: 'Receipts/Invoices Available?', type: 'select', options: ['Yes', 'No'], required: false }
        ]
    },
    {
        id: 'defence_inputs',
        title: 'Known Defence Arguments',
        subtitle: 'Any defence claims made by the accused',
        fields: [
            { name: 'signature_dispute', label: 'Signature Disputed by Accused?', type: 'select', options: ['Yes - Claimed Forged', 'Yes - Claimed Unauthorized', 'No', 'Unknown'], required: false },
            { name: 'debt_denial', label: 'Debt Denied Completely?', type: 'select', options: ['Yes - Complete Denial', 'Partially Denied', 'No', 'Unknown'], required: false },
            { name: 'cheque_security_claim', label: 'Accused Claims Cheque Was Security?', type: 'select', options: ['Yes', 'No', 'Unknown'], required: false },
            { name: 'limitation_claim', label: 'Limitation Period Claimed Expired?', type: 'select', options: ['Yes', 'No', 'Unknown'], required: false },
            { name: 'already_paid_claim', label: 'Accused Claims Already Paid?', type: 'select', options: ['Yes - Full', 'Yes - Partial', 'No', 'Unknown'], required: false },
            { name: 'jurisdiction_challenge', label: 'Jurisdiction Challenged?', type: 'select', options: ['Yes', 'No', 'Unknown'], required: false },
            { name: 'other_defences', label: 'Other Known Defences', type: 'textarea', required: false, placeholder: 'Describe any other defence arguments...' }
        ]
    },
    {
        id: 'negotiations_conduct',
        title: 'Negotiations & Conduct',
        subtitle: 'Settlement attempts and accused behavior',
        fields: [
            { name: 'settlement_attempted', label: 'Out-of-Court Settlement Attempted?', type: 'select', options: ['Yes - Multiple Times', 'Yes - Once', 'No'], required: false },
            { name: 'settlement_amount', label: 'Settlement Amount Discussed (₹)', type: 'number', required: false },
            { name: 'evasive_conduct', label: 'Evasive/Avoiding Conduct?', type: 'select', options: ['Yes - Avoiding Calls', 'Yes - Changed Address', 'Yes - Absconding', 'No'], required: false },
            { name: 'court_attendance', label: 'Is Accused Attending Court Dates?', type: 'select', options: ['Yes - Appearing', 'No - Skipping Dates', 'Not Applicable (Pre-Filing)'], required: false },
            { name: 'counter_claim', label: 'Counter Claim Filed?', type: 'select', options: ['Yes', 'No', 'Threatened'], required: false },
            { name: 'urgency_level', label: 'Case Urgency Level', type: 'select', options: ['Very Urgent', 'Urgent', 'Normal'], required: false },
            { name: 'additional_notes', label: 'Additional Case Notes/Context', type: 'textarea', required: false, placeholder: 'Any other relevant information about the case...' }
        ]
    }
];

export const criminalWizardSteps = [
    {
        id: 'criminal_identity',
        title: 'Criminal Case & FIR Details',
        subtitle: 'FIR details, police station jurisdiction, and statutory framework',
        fields: [
            { name: 'case_id', label: 'FIR / Case Number', type: 'text', required: true, placeholder: 'e.g., FIR No. 204/2026' },
            { name: 'case_title', label: 'Case Title / Caption', type: 'text', required: true, placeholder: 'e.g., State vs Ramesh Kumar & Ors' },
            { name: 'police_station', label: 'Police Station & Jurisdiction', type: 'text', required: true, placeholder: 'e.g., Cyber Crime PS, Bandra, Mumbai' },
            { name: 'client_role', label: 'Client Representation', type: 'select', options: ['Accused', 'Complainant / Informant', 'Victim'], required: true },
            { name: 'statutory_regime', label: 'Statutory Framework', type: 'select', options: ['Bharatiya Nyaya Sanhita (BNS 2023 / BNSS 2023)', 'Indian Penal Code (IPC 1860 / CrPC 1973)', 'Special Criminal Act (NDPS / PMLA / PC Act)'], required: true },
            { name: 'filing_date', label: 'FIR / Complaint Registration Date', type: 'date', required: true },
            { name: 'court_name', label: 'Designated Trial / Remand Court', type: 'text', required: false, placeholder: 'e.g., Additional Sessions Court, Greater Mumbai' }
        ]
    },
    {
        id: 'criminal_offense',
        title: 'Offense & Specific Charges',
        subtitle: 'Primary offense classification, sections charged, and timeline',
        fields: [
            { 
                name: 'offense_type', 
                label: 'Primary Offense Category', 
                type: 'select', 
                options: [
                    'S.420 IPC / S.318 BNS (Cheating & Financial Fraud)', 
                    'S.406/409 IPC / S.316 BNS (Criminal Breach of Trust)', 
                    'S.467/468/471 IPC / S.336/338/340 BNS (Forgery & Valuable Security)', 
                    'S.498A/304B IPC / S.85/80 BNS (Matrimonial Cruelty & Dowry Allegations)', 
                    'S.302/304 IPC / S.103/105 BNS (Homicide / Murder vs Provocation)', 
                    'S.307 IPC / S.109 BNS (Attempt to Murder)', 
                    'S.376 IPC / S.64/69 BNS (Rape / False Promise of Marriage)', 
                    'POCSO Act (Protection of Children from Sexual Offences / S.29 Presumption)',
                    'S.323/324/326 IPC / S.115/117/118 BNS (Voluntarily Causing Hurt / Dangerous Weapon)', 
                    'NDPS Act (Search S.50 & Twin Bail Conditions S.37)', 
                    'Prevention of Corruption Act (Prior Sanction S.17A / S.19)',
                    'PMLA / Money Laundering (ED Scheduled Offense / S.45 Bail)',
                    'Cyber Crime & IT Act (S.66C/66D & Digital Evidence)',
                    'Hit & Run / Rash Driving (S.279/304A IPC ↔ S.281/106 BNS)',
                    'Extortion, Robbery & Dacoity (S.384/392/395 IPC ↔ S.308/309/310 BNS)',
                    'Other Criminal Offense'
                ], 
                required: true 
            },
            { name: 'ipc_section', label: 'Specific Sections Charged (IPC / BNS / Special Acts)', type: 'text', required: true, placeholder: 'e.g., S. 420, 406, 120B IPC / S. 318, 316, 61 BNS' },
            { name: 'incident_date', label: 'Date of Alleged Incident', type: 'date', required: true },
            { name: 'fir_date', label: 'Date of FIR / Complaint', type: 'date', required: true },
            { name: 'delay_explanation', label: 'Explanation for FIR Delay (if any)', type: 'textarea', required: false, placeholder: 'Explain reasons for delay between incident date and FIR registration...' },
            { name: 'max_punishment_years', label: 'Maximum Imprisonment for Highest Offense (Years)', type: 'number', required: true, placeholder: 'e.g., 7' }
        ]
    },
    {
        id: 'criminal_custody',
        title: 'Arrest, Custody & Investigation',
        subtitle: 'Remand status, 41A notice compliance, and default bail milestones',
        fields: [
            { name: 'arrested_during_investigation', label: 'Arrest / Custody Status', type: 'select', options: ['Yes - Currently in Custody', 'Yes - Released on Bail', 'No - Anticipating Arrest / Not Arrested'], required: true },
            { name: 'arrest_date', label: 'Date of Arrest (if applicable)', type: 'date', required: false },
            { name: 'days_in_custody', label: 'Total Days in Judicial / Police Custody', type: 'number', required: false, placeholder: 'e.g., 65' },
            { name: 'no_s41a_notice', label: 'S.41A CrPC / S.35 BNSS Notice Compliance (For offenses <= 7 yrs)', type: 'select', options: ['Notice Served & Cooperated', 'Arrested Directly Without S.41A Notice (Arnesh Kumar Violation)', 'Not Applicable (>7 Years)'], required: true },
            { name: 'chargesheet_filed', label: 'Chargesheet / Final Police Report Status', type: 'select', options: ['No - Investigation Pending (Default Bail S.167 Check)', 'Yes - Chargesheet Submitted in Court'], required: true },
            { name: 'chargesheet_date', label: 'Chargesheet Filing Date (if filed)', type: 'date', required: false },
            { name: 'is_public_servant', label: 'Is Accused a Public Servant / Government Officer?', type: 'select', options: ['No', 'Yes - Public Servant Acting in Official Duty'], required: true },
            { name: 'sanction_obtained', label: 'Prior Sanction Obtained? (S.197 CrPC / S.218 BNSS / S.17A PCA)', type: 'select', options: ['Yes - Valid Sanction on Record', 'No - No Prior Sanction (Cognizance Barred)', 'Not Applicable'], required: true }
        ]
    },
    {
        id: 'criminal_evidence',
        title: 'Evidentiary Audit & Forensics',
        subtitle: 'Electronic certificates, discovery memos, and medical contradictions',
        fields: [
            { name: 'electronic_evidence', label: 'Does Prosecution Rely on Electronic Records (WhatsApp, Calls, CCTV)?', type: 'select', options: ['Yes', 'No'], required: true },
            { name: 's65b_certificate', label: 'Is S.65B IEA / S.63 BSA Certificate Attached?', type: 'select', options: ['Yes - Valid Certificate Available', 'No - Uncertified / Missing (Inadmissible)', 'Not Applicable'], required: false },
            { name: 'recovery_memo_s27', label: 'Discovery / Recovery Memo (S.27 IEA / S.23 BSA)', type: 'select', options: ['Yes - Independent Panch Witnesses Present', 'Yes - Questionable / Stock Witnesses', 'No Recovery Made'], required: false },
            { name: 'medical_contradicts_ocular', label: 'Does Medical / Post-Mortem Report Contradict Eyewitnesses?', type: 'select', options: ['Yes - Direct Medical vs Ocular Contradiction (Thaman Kumar Rule)', 'No - Consistent', 'Not Applicable'], required: false },
            { name: 's161_s164_contradiction', label: 'Major Discrepancy Between S.161 & S.164 Statements?', type: 'select', options: ['Yes - Severe Material Contradiction', 'Minor Variations', 'No'], required: false },
            { name: 'witness_type', label: 'Nature of Key Witnesses', type: 'select', options: ['Independent Public Witnesses', 'Solely Interested / Family Witnesses', 'Only Police Witnesses'], required: false }
        ]
    },
    {
        id: 'criminal_adversarial',
        title: 'Quashing, Bail & Defense Strategy',
        subtitle: 'Bhajan Lal quashing grounds, civil dispute disguise, and relief sought',
        fields: [
            { name: 'contract_exists', label: 'Is There an Underlying Contract / Commercial Dispute? (S.420 Defense)', type: 'select', options: ['Yes - Commercial Contract / Debt Recovery in Criminal Garb', 'No - Pure Criminal Act'], required: true },
            { name: 'relative_impleaded', label: 'Are Distant In-Laws / Relatives Impleaded with Omnibus Claims? (S.498A)', type: 'select', options: ['Yes - Vague Omnibus Allegations on Extended Family', 'No'], required: false },
            { name: 'flight_risk', label: 'Flight Risk / Tampering Apprehension', type: 'select', options: ['No - Deep Local Roots & Clean Track Record', 'Yes - Risk Alleged by Prosecution'], required: false },
            { 
                name: 'primary_relief_sought', 
                label: 'Primary Legal Strategy / Relief Sought', 
                type: 'select', 
                options: [
                    'Anticipatory Bail (S.438 CrPC / S.484 BNSS)', 
                    'Regular Bail (S.437/439 CrPC / S.480/483 BNSS)', 
                    'Default / Statutory Bail (S.167(2) CrPC / S.187 BNSS)', 
                    'High Court Quashing u/s 482 CrPC / S.528 BNSS (Bhajan Lal)', 
                    'Discharge Application u/s 227/239 CrPC / S.250/262 BNSS', 
                    'Trial Cross-Examination & Comprehensive Defense Strategy'
                ], 
                required: true 
            },
            { name: 'additional_notes', label: 'Specific Defense Grounds & Factual Context', type: 'textarea', required: false, placeholder: 'Detail any alibi, hostile witness history, prior settlement agreements, or specific legal objections...' }
        ]
    }
];

export const sarfaesiWizardSteps = [
    {
        id: 'sarfaesi_identity',
        title: 'Loan Account & NPA Particulars',
        subtitle: 'Loan account, borrower details, and NPA classification',
        fields: [
            { name: 'case_id', label: 'Loan Account / SARFAESI ID', type: 'text', required: true, placeholder: 'e.g., SARF/2026/894' },
            { name: 'case_title', label: 'Case Title / Caption', type: 'text', required: true, placeholder: 'e.g., ABC Bank vs XYZ Commercial Infra' },
            { name: 'perspective', label: 'Representation Perspective', type: 'select', options: ['Creditor (Bank / Financial Institution)', 'Borrower / Guarantor / Mortgagor'], required: true },
            { name: 'bank_name', label: 'Secured Creditor (Bank / NBFC Name)', type: 'text', required: true, placeholder: 'e.g., State Bank of India' },
            { name: 'borrower_name', label: 'Borrower / Mortgagor Name', type: 'text', required: true, placeholder: 'e.g., Rajesh Sharma & M/s Sharma Enterprises' },
            { name: 'npa_date', label: 'NPA Classification Date', type: 'date', required: true },
            { name: 'filing_date', label: 'Intake / Audit Date', type: 'date', required: true }
        ]
    },
    {
        id: 'sarfaesi_debt',
        title: 'Debt & Secured Asset Audit',
        subtitle: 'Sanction, outstanding debt, CERSAI, and agricultural land audit',
        fields: [
            { name: 'sanction_amount', label: 'Original Sanction Amount (₹)', type: 'number', required: true, placeholder: 'e.g., 50000000' },
            { name: 'outstanding_amount', label: 'Outstanding Debt Claimed (₹)', type: 'number', required: true, placeholder: 'e.g., 62500000' },
            { name: 'property_description', label: 'Secured Asset / Property Description', type: 'textarea', required: true, placeholder: 'e.g., Commercial Plot No. 45, MIDC Industrial Area, Pune' },
            { name: 'is_agricultural_land', label: 'Is Secured Asset Agricultural Land? (S.31(i) Bar)', type: 'select', options: ['No - Commercial / Residential Property', 'Yes - Agricultural Land (SARFAESI Inapplicable)'], required: true },
            { name: 'cersai_registered', label: 'CERSAI Security Interest Registered? (S.26D)', type: 'select', options: ['Yes - Duly Registered with CERSAI', 'No - Unregistered (Enforcement Barred u/s 26D)'], required: true },
            { name: 'ibc_moratorium', label: 'Is IBC Moratorium / CIRP Active? (S.14 IBC Overrides)', type: 'select', options: ['No', 'Yes - NCLT CIRP / Moratorium Active (Enforcement Barred)'], required: true }
        ]
    },
    {
        id: 'sarfaesi_notices',
        title: 'Section 13 Enforcement Notices',
        subtitle: 'Section 13(2) demand, borrower objection, 13(3A) reply, and 13(4) possession',
        fields: [
            { name: 'notice_13_2_date', label: 'Demand Notice u/s 13(2) Date', type: 'date', required: true },
            { name: 'service_proof_available', label: 'Proof of Service for S.13(2) Notice Available?', type: 'select', options: ['Yes - Acknowledgment / Delivery Proof Available', 'No - Missing Proof of Service'], required: true },
            { name: 'borrower_representation_date', label: 'Borrower Objection / Representation Date (if any)', type: 'date', required: false },
            { name: 'bank_reply_13_3a_date', label: 'Bank Reply u/s 13(3A) Date (Must be within 15 days)', type: 'date', required: false },
            { name: 'possession_13_4_date', label: 'Possession Notice u/s 13(4) Date', type: 'date', required: false }
        ]
    },
    {
        id: 'sarfaesi_auction',
        title: 'Possession & Auction Compliance',
        subtitle: 'Rule 8/9 newspaper publication, valuation, and 30-day notice',
        fields: [
            { name: 'newspaper_publication_done', label: 'Newspaper Publication in 2 Dailies? (Rule 8(6) / 9(1))', type: 'select', options: ['Yes - Published in English & Vernacular Dailies', 'No - Defective Publication', 'Not Applicable (Pre-Auction)'], required: true },
            { name: 'auction_notice_date', label: 'Auction Sale Notice Issued Date', type: 'date', required: false },
            { name: 'auction_date', label: 'Scheduled / Held Auction Date', type: 'date', required: false },
            { name: 'reserve_price', label: 'Approved Reserve Price (₹)', type: 'number', required: false, placeholder: 'e.g., 45000000' }
        ]
    },
    {
        id: 'sarfaesi_drt',
        title: 'DRT Securitization Application (S.17)',
        subtitle: 'DRT filing, 45-day limitation, pre-deposit, and interim stay prayers',
        fields: [
            { name: 'sa_filing_date', label: 'Securitization Application (SA) Filing Date (S.17)', type: 'date', required: false },
            { name: 'drt_court_name', label: 'Designated Debts Recovery Tribunal (DRT Bench)', type: 'text', required: false, placeholder: 'e.g., DRT-I, Mumbai' },
            { name: 'stay_sought', label: 'Interim Stay on Physical Possession / Auction Sought?', type: 'select', options: ['Yes - Urgent Interim Stay Prayed', 'No - Final Hearing / Valuation Challenge Only'], required: false },
            { name: 'additional_notes', label: 'Additional Pleadings & Irregularities Pled', type: 'textarea', required: false, placeholder: 'Detail any undervaluation, lack of panchnama, or breach of mandatory timelines...' }
        ]
    }
];

export const civilWizardSteps = [
    {
        id: 'civil_identity',
        title: 'Civil Suit & Jurisdiction',
        subtitle: 'Suit details, valuation, and court jurisdiction',
        fields: [
            { name: 'case_id', label: 'Suit / Petition Number', type: 'text', required: true, placeholder: 'e.g., Commercial Suit No. 45/2026' },
            { name: 'case_title', label: 'Suit Title', type: 'text', required: true, placeholder: 'e.g., ABC Developers vs XYZ Infra Ltd' },
            { name: 'court_name', label: 'Court / Commercial Court Bench', type: 'text', required: true, placeholder: 'e.g., City Civil Court, Commercial Division' },
            { name: 'suit_valuation', label: 'Suit Valuation / Claim Amount (₹)', type: 'number', required: true, placeholder: 'e.g., 50000000' },
            { name: 's12a_mediation', label: 'Pre-Institution Mediation u/s 12A Commercial Courts Act?', type: 'select', options: ['Yes - Mediation Attempted / Failed', 'No - Urgent Interim Relief Claimed', 'Not a Commercial Suit'], required: true },
            { name: 'filing_date', label: 'Filing Date', type: 'date', required: true }
        ]
    },
    {
        id: 'civil_cause_of_action',
        title: 'Cause of Action & Contract Details',
        subtitle: 'Agreement terms, breach date, and limitation audit',
        fields: [
            { name: 'agreement_date', label: 'Agreement / Contract Execution Date', type: 'date', required: true },
            { name: 'breach_date', label: 'Date Cause of Action Arose / Breach Date', type: 'date', required: true },
            { name: 'agreement_registered', label: 'Is Agreement Stamped & Registered?', type: 'select', options: ['Yes - Duly Stamped & Registered', 'Unstamped / Insufficiently Stamped (Impounding Risk)', 'Oral Agreement'], required: true },
            { name: 'limitation_article', label: 'Limitation Act Article Applicable', type: 'select', options: ['Article 54 - Specific Performance (3 Years)', 'Article 55 - Breach of Contract (3 Years)', 'Article 113 - Residual Civil Suits (3 Years)', 'Article 65 - Possession of Immovable Property (12 Years)'], required: true }
        ]
    },
    {
        id: 'civil_relief',
        title: 'Relief Sought & Interim Applications',
        subtitle: 'Primary prayers and Order 39 injunction requirements',
        fields: [
            { name: 'primary_prayer', label: 'Primary Relief Sought', type: 'select', options: ['Specific Performance of Contract', 'Recovery of Money / Commercial Damages', 'Permanent & Mandatory Injunction', 'Declaration of Title & Ownership', 'Cancellation of Sale Deed'], required: true },
            { name: 'order_39_injunction', label: 'Interim Injunction Sought? (Order 39 Rules 1 & 2 CPC)', type: 'select', options: ['Yes - Prima Facie Case & Balance of Convenience Pled', 'No - Only Final Decree Sought'], required: true },
            { name: 'additional_notes', label: 'Additional Pleadings / Submissions', type: 'textarea', required: false, placeholder: 'Describe readiness and willingness (S.16(c) SRA) or breach particulars...' }
        ]
    }
];

export const compositeWizardSteps = [
    {
        id: 'composite_identity',
        title: 'Loan Account & Borrower Identity',
        subtitle: 'Master loan account and multi-track enforcement details',
        fields: [
            { name: 'case_id', label: 'Loan Account / Reference ID', type: 'text', required: true, placeholder: 'e.g., NPA/MULTITRACK/2026/01' },
            { name: 'case_title', label: 'Matter Title / Caption', type: 'text', required: true, placeholder: 'e.g., Bank / Creditor vs Defaulting Borrower' },
            { name: 'bank_name', label: 'Secured Creditor / Complainant Institution', type: 'text', required: true, placeholder: 'e.g., State Bank of India / HDFC Bank' },
            { name: 'borrower_name', label: 'Borrower / Accused Entity / Guarantor', type: 'text', required: true, placeholder: 'e.g., Rajesh Sharma & Apex Infra Ltd' },
            { name: 'perspective', label: 'Representation Perspective', type: 'select', options: ['Creditor (Bank / Financial Institution / Complainant)', 'Borrower / Accused / Mortgagor'], required: true },
            { name: 'outstanding_amount', label: 'Total Outstanding Debt (₹)', type: 'number', required: true, placeholder: 'e.g., 50000000' },
            { name: 'case_type', label: 'Case Type', type: 'select', options: ['Multi-Track (SARFAESI + 138 + Criminal)', 'SARFAESI', 'Cheque Bounce', 'Criminal', 'Civil'], required: true },
            { name: 'filing_date', label: 'Evaluation / Intake Date', type: 'date', required: true }
        ]
    },
    {
        id: 'composite_sarfaesi_track',
        title: 'Track 1: SARFAESI & Mortgaged Collateral',
        subtitle: 'NPA classification, Section 13 notices, and CERSAI security check',
        fields: [
            { name: 'npa_date', label: 'NPA Classification Date', type: 'date', required: true },
            { name: 'notice_13_2_date', label: 'Section 13(2) Demand Notice Date', type: 'date', required: true },
            { name: 'cersai_registered', label: 'CERSAI Security Interest Registered? (S.26D Mandate)', type: 'select', options: ['Yes - Duly Registered with CERSAI', 'No - Unregistered (Enforcement Barred u/s 26D)'], required: true },
            { name: 'property_description', label: 'Secured Mortgaged Asset Details', type: 'textarea', required: true, placeholder: 'e.g., Commercial Unit No. 102, MIDC Industrial Area, Pune' },
            { name: 'is_agricultural_land', label: 'Is Collateral Agricultural Land? (S.31(i) Bar)', type: 'select', options: ['No - Commercial / Residential Property', 'Yes - Agricultural Land (SARFAESI Inapplicable)'], required: true },
            { name: 'possession_13_4_date', label: 'Section 13(4) Possession Notice Date (if taken)', type: 'date', required: false }
        ]
    },
    {
        id: 'composite_cb_track',
        title: 'Track 2: Section 138 Dishonoured Cheque',
        subtitle: 'Cheque particulars, return memo, and statutory demand notice',
        fields: [
            { name: 'cheque_present', label: 'Original Cheque Available?', type: 'select', options: ['Yes', 'No'], required: true },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true, placeholder: 'e.g., 409821' },
            { name: 'cheque_amount', label: 'Cheque Amount (₹)', type: 'number', required: true, placeholder: 'e.g., 15000000' },
            { name: 'date_of_dishonour', label: 'Cheque Dishonour Date', type: 'date', required: true },
            { name: 'dishonour_memo', label: 'Bank Return Memo Available?', type: 'select', options: ['Yes', 'No'], required: true },
            { name: 'notice_sent', label: 'Section 138 Statutory Legal Demand Notice Sent?', type: 'select', options: ['Yes - Sent Within 30 Days', 'No - Notice Delayed / Not Sent'], required: true },
            { name: 'date_of_notice', label: 'Statutory Notice Dispatch Date', type: 'date', required: true }
        ]
    },
    {
        id: 'composite_criminal_track',
        title: 'Track 3: Criminal Deceit & Asset Misappropriation',
        subtitle: 'Fraudulent intention at inception, siphoning of funds, or alienation of security',
        fields: [
            { name: 'contract_exists', label: 'Was Debt Incurred Under Formal Loan Agreement?', type: 'select', options: ['Yes', 'No'], required: true },
            { name: 'entrustment_proven', label: 'Were Collateral / Funds Entrusted to Accused? (S.316 BNS / S.406 IPC)', type: 'select', options: ['Yes', 'No'], required: true },
            { name: 'alienation_of_hypothecated_assets', label: 'Did Borrower Dishonestly Remove / Sell Hypothecated Assets?', type: 'select', options: ['Yes - Fraudulent Alienation / Missing Security', 'No - Collateral Intact', 'Suspected / Under Audit'], required: true },
            { name: 'offense_type', label: 'Primary Criminal Allegation', type: 'select', options: ['Cheating & Alienation of Hypothecated Stocks (BNS 318/316 ↔ IPC 420/406)', 'Fabrication / Forged Financial Documentation (BNS 336 ↔ IPC 467/471)', 'Multiple Mortgages on Same Collateral', 'General Financial Fraud'], required: true }
        ]
    },
    {
        id: 'composite_remedies_strategy',
        title: 'Concurrent Legal Strategy & Fast-Track Relief',
        subtitle: 'Section 143A interim compensation and Section 14 physical possession',
        fields: [
            { name: 's143a_interim_sought', label: 'Apply for 20% Interim Cash Compensation u/s 143A NI Act?', type: 'select', options: ['Yes - Urgent Application Upon Framing Notice', 'No'], required: true },
            { name: 'sec14_dm_application_filed', label: 'File Section 14 Application to DM/CMM for Physical Possession?', type: 'select', options: ['Yes - Apply with 9-Point Affidavit', 'No - Symbolic Possession Sufficient'], required: true },
            { name: 'additional_notes', label: 'Special Instructions / Cross-Track Notes', type: 'textarea', required: false, placeholder: 'Enter any specific DRT stay orders, settlement negotiations, or cross-court records...' }
        ]
    }
];

export function getActiveWizardSteps(caseType) {
    // Only Section 138 (Cheque Bounce) wizard is active
    return wizardSteps;
}



export const roleActions = {
    law_firm: [
        { title: 'Run Analysis', description: 'Scan for FATAL defects and map courtroom strategy', icon: 'fa-search', color: '#ef4444', action: 'startCaseAnalysis' },
        { title: 'Generate Draft', description: 'Generate court-ready legal drafts', icon: 'fa-file-contract', color: '#10b981', action: 'generateDraft' }
    ],
    in_house: [
        { title: 'Run Analysis', description: 'Scan for FATAL defects and map courtroom strategy', icon: 'fa-search', color: '#ef4444', action: 'startCaseAnalysis' },
        { title: 'Generate Draft', description: 'Generate court-ready legal drafts', icon: 'fa-file-contract', color: '#10b981', action: 'generateDraft' }
    ],
    corporate_legal: [
        { title: 'Run Analysis', description: 'Scan for FATAL defects and map courtroom strategy', icon: 'fa-search', color: '#ef4444', action: 'startCaseAnalysis' },
        { title: 'Generate Draft', description: 'Generate court-ready legal drafts', icon: 'fa-file-contract', color: '#10b981', action: 'generateDraft' }
    ],
    research: [
        { title: 'Run Analysis', description: 'Scan for FATAL defects and map courtroom strategy', icon: 'fa-search', color: '#ef4444', action: 'startCaseAnalysis' },
        { title: 'Generate Draft', description: 'Generate court-ready legal drafts', icon: 'fa-file-contract', color: '#10b981', action: 'generateDraft' }
    ],
    citizen: [
        { title: 'Run Analysis', description: 'Scan for FATAL defects and map courtroom strategy', icon: 'fa-search', color: '#ef4444', action: 'startCaseAnalysis' },
        { title: 'Generate Draft', description: 'Generate court-ready legal drafts', icon: 'fa-file-contract', color: '#10b981', action: 'generateDraft' }
    ],
    lawyer: [
        { title: 'Run Analysis', description: 'Scan for FATAL defects and map courtroom strategy', icon: 'fa-search', color: '#ef4444', action: 'startCaseAnalysis' },
        { title: 'Generate Draft', description: 'Generate court-ready legal drafts', icon: 'fa-file-contract', color: '#10b981', action: 'generateDraft' }
    ],
    student: [
        { title: 'Run Analysis', description: 'Scan for FATAL defects and map courtroom strategy', icon: 'fa-search', color: '#ef4444', action: 'startCaseAnalysis' },
        { title: 'Generate Draft', description: 'Generate court-ready legal drafts', icon: 'fa-file-contract', color: '#10b981', action: 'generateDraft' }
    ]
};

// ── CMS Configuration & Enums ─────────────────────────────────
export const caseTypes = [
    { value: 'section_138', label: 'Section 138 (Cheque Bounce)', icon: 'fa-file-invoice-dollar', color: '#3b82f6' }
];

export const caseStatuses = [
    { value: 'draft', label: 'Draft', color: '#6b7280' },
    { value: 'ongoing', label: 'Ongoing', color: '#3b82f6' },
    { value: 'resolved', label: 'Resolved', color: '#10b981' },
    { value: 'archived', label: 'Archived', color: '#9ca3af' }
];

export const casePriorities = [
    { value: 'high', label: 'High', color: '#ef4444' },
    { value: 'medium', label: 'Medium', color: '#f59e0b' },
    { value: 'low', label: 'Low', color: '#6b7280' }
];

export const documentTypes = [
    { value: 'cheque', label: 'Original Dishonoured Cheque' },
    { value: 'bank_return_memo', label: 'Bank Return / Dishonour Memo' },
    { value: 'demand_notice', label: 'Statutory Demand Notice (S.138 / S.13(2))' },
    { value: 'postal_receipt', label: 'Postal Dispatch / AD Card / Tracking' },
    { value: 'court_order', label: 'Court Order / Interim Injunction' },
    { value: 'loan_agreement', label: 'Sanction Letter / Loan Agreement' },
    { value: 'mortgage_deed', label: 'Mortgage Deed / Title Deed' },
    { value: 'valuation_report', label: 'Valuation & Inspection Report' },
    { value: 'affidavit', label: 'Sworn Affidavit / Declaration' },
    { value: 'bsa_certificate', label: 'S.65B IEA / S.63 BSA Certificate' },
    { value: 'other', label: 'Other Document' }
];

export const clientTypes = [
    { value: 'bank', label: 'Bank / Scheduled Commercial Bank' },
    { value: 'nbfc', label: 'NBFC / Financial Institution' },
    { value: 'company', label: 'Private / Public Limited Company' },
    { value: 'individual', label: 'Individual / Proprietor' }
];

