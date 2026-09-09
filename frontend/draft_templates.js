function verifyDraftTimelineS138(d) {
    if (!d) return { auditReport: '', warnings: [] };
    const parse = (s) => s ? new Date(s) : null;
    const daysBetween = (d1, d2) => {
        if (!d1 || !d2 || isNaN(d1) || isNaN(d2)) return null;
        return Math.round((d2 - d1) / (1000 * 60 * 60 * 24));
    };

    const chequeDt = parse(d.cheque_date);
    const presDt = parse(d.presentation_date || d.dishonour_date);
    const disDt = parse(d.dishonour_date);
    const noticeDt = parse(d.notice_date);
    const noticeRecDt = parse(d.notice_served_date || d.notice_received_date || d.notice_date);
    const filingDt = parse(d.filing_date);

    const auditLines = [];
    const warnings = [];

    if (chequeDt && presDt) {
        const gap = daysBetween(chequeDt, presDt);
        if (gap !== null && gap > 92) {
            const msg = `Cheque presented on day ${gap} (> 90 days statutory validity from cheque date).`;
            warnings.push(msg);
            auditLines.push(`[🚨 TIMELINE BREACH - CHEQUE EXPIRED]: ${msg}`);
        } else if (gap !== null && gap >= 0) {
            auditLines.push(`[✓ CHEQUE VALIDITY]: Presented within ${gap} days of issue.`);
        }
    }

    if (disDt && noticeDt) {
        const gap = daysBetween(disDt, noticeDt);
        if (gap !== null && gap > 30) {
            const msg = `Statutory Demand Notice dispatched on day ${gap} (> 30-day statutory limit from dishonour date). Notice is time-barred!`;
            warnings.push(msg);
            auditLines.push(`[🚨 TIMELINE BREACH - NOTICE DELAYED]: ${msg}`);
        } else if (gap !== null && gap >= 0) {
            auditLines.push(`[✓ NOTICE DISPATCH]: Notice dispatched on day ${gap} (within 30-day statutory window).`);
        }
    }

    if (noticeRecDt && filingDt) {
        const gap = daysBetween(noticeRecDt, filingDt);
        if (gap !== null) {
            if (gap < 15) {
                const msg = `Complaint filed on day ${gap} post-notice (< 15 days). Premature filing before cause of action accrued u/s 138(c).`;
                warnings.push(msg);
                auditLines.push(`[🚨 TIMELINE WARNING - PREMATURE FILING]: ${msg}`);
            } else if (gap > 45) {
                const delay = gap - 45;
                const msg = `Complaint filed on day ${gap} post-notice (${delay} days past 30-day window). Requires Condonation of Delay u/s 142(1)(b) NI Act.`;
                warnings.push(msg);
                auditLines.push(`[⚠️ TIMELINE ALERT - CONDONATION REQUIRED]: ${msg}`);
            } else if (gap >= 15) {
                auditLines.push(`[✓ COMPLAINT TIMELINE]: Filed on day ${gap} post-notice service (within statutory filing window).`);
            }
        }
    }

    let report = '';
    if (auditLines.length > 0) {
        report = `======================================================================\nSECTION 138 NI ACT STATUTORY TIMELINE AUDIT REPORT\n======================================================================\n` + auditLines.join('\n');
        if (warnings.length > 0) {
            report += `\n\nSUMMARY STATUTORY WARNINGS:\n` + warnings.map(w => `  • ${w}`).join('\n');
        }
        report += `\n======================================================================\n\n`;
    }

    return { auditReport: report, warnings };
}

const DRAFT_TYPES = [
    {
        id: 'demand_notice', number: 1, title: 'Legal Demand Notice', subtitle: 'Pre-Complaint',
        description: 'Sent within 30 days of cheque dishonour. Mandatory before filing.',
        icon: 'fa-envelope-open-text', color: '#0ea5e9',
        fields: [
            { name: 'complainant_name', label: 'Complainant Full Name', type: 'text', required: true, placeholder: 'e.g., Ramesh Kumar Sharma' },
            { name: 'complainant_address', label: 'Complainant Address', type: 'textarea', required: true, placeholder: 'Full postal address...' },
            { name: 'accused_name', label: 'Accused Full Name', type: 'text', required: true, placeholder: 'e.g., Suresh Mohan Gupta' },
            { name: 'accused_address', label: 'Accused Address', type: 'textarea', required: true, placeholder: 'Full postal address...' },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true, placeholder: 'e.g., 012345' },
            { name: 'cheque_date', label: 'Cheque Date', type: 'date', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (₹)', type: 'number', required: true, placeholder: 'e.g., 250000' },
            { name: 'bank_name', label: 'Bank Name', type: 'text', required: true, placeholder: 'e.g., State Bank of India' },
            { name: 'branch_name', label: 'Branch Name', type: 'text', required: false, placeholder: 'e.g., Andheri West Branch' },
            { name: 'dishonour_date', label: 'Dishonour Date', type: 'date', required: true },
            { name: 'dishonour_reason', label: 'Reason for Dishonour', type: 'text', required: true, placeholder: 'e.g., Insufficient Funds' },
            { name: 'transaction_purpose', label: 'Purpose of Transaction / Underlying Debt', type: 'textarea', required: true, placeholder: 'Describe the loan/transaction for which the cheque was issued...' },
            { name: 'notice_date', label: 'Date of This Notice', type: 'date', required: true },
            { name: 'demand_days', label: 'Payment Demand Period', type: 'select', required: true, options: ['15 days', '30 days'] }
        ],
        generate: function (d) {
            const timelineCheck = verifyDraftTimelineS138(d);
            const body = `LEGAL NOTICE UNDER SECTION 138 OF THE NEGOTIABLE INSTRUMENTS ACT, 1881

Date: ${formatDraftDate(d.notice_date)}

To,
${d.accused_name}
${d.accused_address}

Subject: Legal Notice under Section 138 of the Negotiable Instruments Act, 1881 for dishonour of Cheque No. ${d.cheque_number} dated ${formatDraftDate(d.cheque_date)} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/-

Sir/Madam,

Under the instructions and authority of my client, ${d.complainant_name}, residing at ${d.complainant_address} (hereinafter referred to as the "Complainant"), I hereby issue you this Legal Notice as under:

1. That my client and you had a business/financial relationship, wherein my client advanced a sum of Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- (Rupees ${numberToWords(d.cheque_amount)} Only) to you towards ${d.transaction_purpose}.

2. That in discharge of your legally enforceable liability, you issued Cheque No. ${d.cheque_number} dated ${formatDraftDate(d.cheque_date)}, for a sum of Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- (Rupees ${numberToWords(d.cheque_amount)} Only), drawn on ${d.bank_name}${d.branch_name ? ', ' + d.branch_name : ''}.

3. That the said cheque, when presented for encashment, was returned/dishonoured on ${formatDraftDate(d.dishonour_date)} with the bank's memo stating the reason: "${d.dishonour_reason}".

4. That the dishonour of the said cheque amounts to an offence under Section 138 of the Negotiable Instruments Act, 1881, as amended from time to time.

5. That you are, therefore, called upon to make payment of Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- (Rupees ${numberToWords(d.cheque_amount)} Only) to my client within ${d.demand_days} from the receipt of this notice, failing which my client shall be constrained to initiate criminal as well as civil proceedings against you, at your cost and risk, including prosecution under Section 138 of the Negotiable Instruments Act, 1881, without any further notice.

6. That this Notice is being sent to you by Registered Post A.D. / Speed Post, in compliance with the statutory requirements of Section 138 of the NI Act.

Please treat this as a final opportunity to settle the matter amicably.

Yours faithfully,

${d.complainant_name}
(Complainant / Through Advocate)

Copy to: Complainant (for records)

Note: Date of receipt of this notice shall be the date of service for the purpose of computing the limitation period under Section 138 NI Act.`;
            return timelineCheck.auditReport ? timelineCheck.auditReport + body : body;
        }
    },
    {
        id: 'CERTIFICATE_BSA', number: 13, title: 'S.63(4) BSA Certificate', subtitle: 'Digital Evidence Admissibility',
        description: 'Mandatory certificate required under Bharatiya Sakshya Adhiniyam to admit electronic records (WhatsApp, Email) in court.',
        icon: 'fa-file-signature', color: '#10b981',
        fields: [
            { name: 'complainant_name', label: 'Complainant Full Name', type: 'text', required: true, placeholder: 'e.g., Ramesh Kumar Sharma' },
            { name: 'device_type', label: 'Device Used', type: 'text', required: true, placeholder: 'e.g., Samsung Galaxy S23 / Dell Inspiron Laptop' },
            { name: 'court_location', label: 'Court Location', type: 'text', required: true, placeholder: 'e.g., New Delhi' },
            { name: 'accused_name', label: 'Accused Name', type: 'text', required: true, placeholder: 'e.g., Suresh Mohan Gupta' }
        ],
        generate: function(d) {
            return `IN THE COURT OF THE LEARNED JUDICIAL MAGISTRATE
AT ${d.court_location || '________'}

COMPLAINT NO.: _____ / ${new Date().getFullYear()}

IN THE MATTER OF:
${d.complainant_name || '________'}                                              ... COMPLAINANT
VERSUS
${d.accused_name || '________'}                                             ... ACCUSED

AFFIDAVIT / CERTIFICATE UNDER SECTION 63(4) OF THE BHARATIYA SAKSHYA ADHINIYAM (BSA) FOR ADMISSIBILITY OF ELECTRONIC RECORDS

I, ${d.complainant_name || '________'}, adult, do hereby solemnly affirm and state as under:

1. That I am the Complainant in the above-mentioned matter and I am fully conversant with the facts and circumstances of the case. I am competent to swear this affidavit/certificate.

2. That the electronic records (WhatsApp chats, Emails, Bank Statements) produced along with the complaint have been extracted from my device, namely: ${d.device_type || '________'}, which is owned, maintained, and operated exclusively by me in the ordinary course of my everyday activities.

3. That during the period to which the electronic records relate, the said device was functioning properly. At no point was the device subject to any operational failure or malfunction that could have affected the accuracy, integrity, or contents of the electronic records so generated and stored.

4. That I have personally taken the printouts / screenshots from the aforementioned device, and no alteration, modification, or tampering has been done to the data.

5. That the contents of the printouts are a true and accurate reproduction of the electronic records as stored in the device.

6. That this certificate is being furnished in strict compliance with the mandatory provisions of Section 63(4) of the Bharatiya Sakshya Adhiniyam (BSA), to establish the authenticity and admissibility of the digital evidence relied upon in the present complaint.

DEPONENT
${d.complainant_name || '________'}

VERIFICATION
I, the above-named Deponent, do hereby verify that the contents of paragraphs 1 to 6 of this Affidavit/Certificate are true and correct to the best of my knowledge and belief. No part of it is false and nothing material has been concealed therefrom.

Verified at ${d.court_location || '________'} on this ____ day of ____________, ${new Date().getFullYear()}.

DEPONENT`;
        }
    },
    {
        id: 'reply_notice', number: 2, title: 'Reply to Legal Notice', subtitle: 'Accused Side',
        description: 'Used by the defendant to deny liability or raise a legal defence.',
        icon: 'fa-reply', color: '#8b5cf6',
        fields: [
            { name: 'accused_name', label: 'Accused Full Name (Replying Party)', type: 'text', required: true },
            { name: 'accused_address', label: 'Accused Address', type: 'textarea', required: true },
            { name: 'complainant_name', label: 'Complainant Name', type: 'text', required: true },
            { name: 'notice_date', label: 'Date of Original Notice Received', type: 'date', required: true },
            { name: 'reply_date', label: 'Date of This Reply', type: 'date', required: true },
            { name: 'cheque_number', label: 'Cheque Number (as mentioned in notice)', type: 'text', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (Rs.)', type: 'number', required: true },
            { name: 'defence_ground', label: 'Primary Defence Ground', type: 'select', required: true, options: ['Cheque was given as Security - No Debt', 'Debt already fully repaid', 'Cheque was blank - misused', 'Signature is forged/disputed', 'No legally enforceable debt existed', 'Cheque issued under coercion/pressure', 'Other'] },
            { name: 'defence_details', label: 'Detailed Defence / Facts', type: 'textarea', required: true, placeholder: 'Explain your defence in detail...' },
            { name: 'advocate_name', label: "Accused's Advocate Name (Optional)", type: 'text', required: false }
        ],
        generate: function (d) {
            let caseCite = '';
            if (d.defence_ground.includes('Security')) {
                caseCite = '\n\nThat as held by the Hon\'ble Supreme Court in "Sampelly Satyanarayana Rao v. IREDA (2016) 10 SCC 458", a cheque given as security cannot be used for prosecution unless the debt is crystallised. In the present case, no such debt exists.';
            } else if (d.defence_ground.includes('Signature')) {
                caseCite = '\n\nThat as held in "Laxmi Dyechem v. State of Gujarat (2012) 13 SCC 375", the burden of proving a genuine signature lies heavily on the complainant, and any mismatch entitles the accused to a presumption of innocence.';
            }

            return `REPLY TO LEGAL NOTICE

Date: ${formatDraftDate(d.reply_date)}

To,
${d.complainant_name}
(And/Or through their Advocate)

Subject: Reply to your Legal Notice dated ${formatDraftDate(d.notice_date)} pertaining to Cheque No. ${d.cheque_number} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/-

Sir/Madam,

I, ${d.accused_name}, residing at ${d.accused_address}, hereby issue this Reply to your Notice dated ${formatDraftDate(d.notice_date)} as under:

1. At the outset, I deny each and every allegation, averment, and statement made in your notice, except those which are specifically admitted herein. The same are denied, both in law and on facts.

2. DENIAL OF LIABILITY: The contents of your notice are false, frivolous, vexatious, and devoid of any legal merit. The notice has been issued with the malafide intention of harassing and extorting money from me.

3. DEFENCE GROUND - ${d.defence_ground.toUpperCase()}:
${d.defence_details}${caseCite}

4. LEGAL POSITION: The cheque in question (No. ${d.cheque_number} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/-) was NOT issued in discharge of any legally enforceable debt or liability as alleged. The fundamental ingredients required under Section 138 of the Negotiable Instruments Act, 1881 are not fulfilled in the present case.

5. That your notice is, therefore, legally untenable, misconceived, and not maintainable in law. Any action initiated on the basis of your illegal and frivolous notice shall be resisted by me with full force and at your cost and risk.

6. You are hereby put on notice that if you proceed with any criminal complaint or civil action, I shall be compelled to take appropriate legal action for malicious prosecution, defamation, and seek damages accordingly.

7. I reserve all my legal rights and remedies in this matter.

Yours faithfully,

${d.accused_name}
${d.accused_address}
${d.advocate_name ? '\nThrough Advocate: ' + d.advocate_name : ''}

(This Reply is sent without prejudice to all rights and remedies available to the sender)`;
        }
    },
    {
        id: 'criminal_complaint', number: 3, title: 'Criminal Complaint (Sec. 138)', subtitle: 'Main Filing',
        description: 'Main complaint filed before the Magistrate with all facts and legal grounds.',
        icon: 'fa-gavel', color: '#ef4444',
        fields: [
            { name: 'court_name', label: 'Court Name', type: 'text', required: true, placeholder: 'e.g., Court of Judicial Magistrate First Class, Pune' },
            { name: 'complainant_name', label: 'Complainant Full Name', type: 'text', required: true },
            { name: 'complainant_address', label: 'Complainant Address', type: 'textarea', required: true },
            { name: 'complainant_occupation', label: 'Complainant Occupation', type: 'text', required: false, placeholder: 'e.g., Businessman' },
            { name: 'accused_name', label: 'Accused Full Name', type: 'text', required: true },
            { name: 'accused_address', label: 'Accused Address', type: 'textarea', required: true },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true },
            { name: 'cheque_date', label: 'Cheque Date', type: 'date', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (Rs.)', type: 'number', required: true },
            { name: 'bank_name', label: 'Drawer Bank Name', type: 'text', required: true },
            { name: 'branch_name', label: 'Branch Name', type: 'text', required: false },
            { name: 'presentation_date', label: 'Date of Cheque Presentation', type: 'date', required: true },
            { name: 'dishonour_date', label: 'Date of Dishonour', type: 'date', required: true },
            { name: 'dishonour_reason', label: 'Dishonour Reason (as per bank memo)', type: 'text', required: true },
            { name: 'notice_date', label: 'Date of Demand Notice Sent', type: 'date', required: true },
            { name: 'notice_mode', label: 'Mode of Notice', type: 'select', required: true, options: ['Registered Post A.D.', 'Speed Post', 'Registered Post + Email', 'Hand Delivery with Acknowledgement'] },
            { name: 'notice_served', label: 'Was Notice Served?', type: 'select', required: true, options: ['Yes - Accused Acknowledged', 'Yes - Refused by Accused', 'Returned Unserved (deemed served)', 'Unknown'] },
            { name: 'payment_made', label: 'Did Accused Pay After Notice?', type: 'select', required: true, options: ['No - No Payment', 'Partial Payment Only'] },
            { name: 'transaction_purpose', label: 'Underlying Debt / Transaction Details', type: 'textarea', required: true },
            { name: 'filing_date', label: 'Date of Filing This Complaint', type: 'date', required: true }
        ],
        generate: function (d) {
            const timelineCheck = verifyDraftTimelineS138(d);
            let s141_averment = '';
            const directorsNamed = window.state?.caseData?.directors_named || 'No';
            const roleCat = window.state?.caseData?.director_role_category || 'Managing Director / Whole-Time Director (Inherent Liability)';
            const activeAverment = window.state?.caseData?.active_management_averment || 'Yes - Expressly averred in charge of day-to-day business (S.M.S. Pharma Standard)';
            const isNamed = ['yes', 'yes - company as a1 and directors/partners named', 'yes - actively managed operations', 'yes - partial'].includes((String(directorsNamed)).toLowerCase());
            
            if (d.accused_type === 'Company' || d.accused_type === 'Partnership Firm' || d.accused_type === 'Pvt Ltd/Ltd Company') {
                if (isNamed) {
                    s141_averment = `\n\n11. VICARIOUS LIABILITY (SECTION 141 NI ACT — S.M.S. PHARMACEUTICALS TEST):
    (a) That the Accused No. 1 is a duly incorporated corporate entity/firm and the Accused Nos. 2 onwards are its Directors/Partners/Designated Officers.
    (b) That at the relevant time when the debt was incurred, the dishonoured cheque was issued, and statutory notice was served, Accused No. 2 [${roleCat}] was actively in charge of, and directly responsible to the Accused No. 1 for the day-to-day management and conduct of its business.
    (c) Averment per S.M.S. Pharmaceuticals Ltd. v. Neeta Bhalla (2005) 8 SCC 89: "${activeAverment}". The company Accused No. 1 has been impleaded as the principal accused strictly satisfying the mandate of Aneeta Hada v. Godfather Travels (2012) 5 SCC 661.`;
                } else {
                    s141_averment = `\n\n11. VICARIOUS LIABILITY (SECTION 141 NI ACT):
    [FATAL DEFECT WARNING: You must arraign the Company as Accused No. 1 and expressly aver that the specific Directors were in charge of and responsible for day-to-day business (S.M.S. Pharmaceuticals test). Failure to do so triggers mandatory quashing under Aneeta Hada.]`;
                }
            }

            const bsaAnnexure = `

================================================================================
ANNEXURE-C: CERTIFICATE UNDER SECTION 63(4) BHARATIYA SAKSHYA ADHINIYAM, 2023
(MANDATORY DIGITAL EVIDENCE CERTIFICATION — FORMERLY S.65B IEA)
================================================================================

I, ${d.complainant_name}, do hereby solemnly state, affirm and certify as under:

1. That I am the Complainant in the accompanying Section 138 Criminal Complaint and am fully conversant with the electronic records relied upon herein (including computerized bank return memos, digital account statements, and WhatsApp/Email communications with the Accused).

2. That the electronic records produced and annexed herewith were produced by computer systems/digital devices during the regular course of activities and periods over which I had lawful operational control.

3. That the digital reproduction annexed as Annexure-B & Annexure-C is a true, unmodified, and authentic printout/replica of the electronic records stored on the said device, and the integrity of the data has remained intact throughout without tampering or unauthorized modification.

4. This certificate is executed in strict compliance with Section 63(4) of the Bharatiya Sakshya Adhiniyam, 2023 (BSA) to render the electronic evidence legally admissible before this Hon'ble Court.

DEPONENT: ______________________
DATE: ${formatDraftDate(d.filing_date)}
PLACE: ${d.court_name ? d.court_name.split(',')[0] : '_______________'}`;

            const body = `IN THE ${d.court_name.toUpperCase()}

CRIMINAL COMPLAINT UNDER SECTION 138 READ WITH SECTION 142 OF
THE NEGOTIABLE INSTRUMENTS ACT, 1881

COMPLAINT CASE NO. _____________ / ${new Date(d.filing_date || Date.now()).getFullYear()}

IN THE MATTER OF:

COMPLAINANT:
${d.complainant_name}
${d.complainant_address}
${d.complainant_occupation ? '(Occupation: ' + d.complainant_occupation + ')' : ''}
                                                ...Complainant

VERSUS

ACCUSED:
${d.accused_name}
${d.accused_address}
                                                ...Accused

-------------------------------------------------

CRIMINAL COMPLAINT UNDER SECTION 138 OF THE NEGOTIABLE INSTRUMENTS ACT, 1881

-------------------------------------------------

MOST RESPECTFULLY SHEWETH:

1. That the Complainant, ${d.complainant_name}, is ${d.complainant_occupation ? 'engaged in ' + d.complainant_occupation : 'a law-abiding citizen/entity'}. ${d.accused_type === 'Company' ? 'The Complainant is represented by its Authorized Signatory/Director, who is duly empowered to file, sign, and verify the present complaint by way of a Board Resolution/Letter of Authority dated _________, which is produced herewith as Annexure-A.' : ''}

2. That the Accused, ${d.accused_name}, is known to the Complainant and both parties had a financial/business relationship with each other.

3. UNDERLYING TRANSACTION & S.139 PRESUMPTION:
   That the Complainant advanced/paid a sum of Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- (Rupees ${numberToWords(d.cheque_amount)} Only) to the Accused towards ${d.transaction_purpose}. In terms of Section 139 of the NI Act, a statutory presumption operates in favour of the holder that the cheque was issued in discharge of a legally enforceable debt/liability (Rangappa v. Sri Mohan).

4. ISSUANCE OF CHEQUE:
   That in discharge of the aforesaid legally enforceable debt/liability, the Accused issued Cheque No. ${d.cheque_number} dated ${formatDraftDate(d.cheque_date)} for an amount of Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- (Rupees ${numberToWords(d.cheque_amount)} Only) drawn on ${d.bank_name}${d.branch_name ? ', ' + d.branch_name : ''}.

5. PRESENTATION & DISHONOUR:
   That the Complainant presented the said cheque for encashment on ${formatDraftDate(d.presentation_date)}. However, the said cheque was returned/dishonoured by the bank on ${formatDraftDate(d.dishonour_date)} with the bank's memo stating: "${d.dishonour_reason}."

6. LEGAL DEMAND NOTICE:
   That as required under Section 138 of the NI Act, the Complainant sent a Legal Demand Notice to the Accused on ${formatDraftDate(d.notice_date)} by way of ${d.notice_mode}. The notice was ${d.notice_served}. As per "C.C. Alavi Haji v. Palapetty Muhammed (2007) 6 SCC 555", the service of notice is complete once dispatched to the correct address.

7. FAILURE TO PAY & CAUSE OF ACTION:
   That despite receipt of the said notice, the Accused has ${d.payment_made}. The 15-day statutory payment window has elapsed. The cause of action arose on the 16th day after service in strict compliance with Section 9 of the General Clauses Act, 1897 and Yogendra Pratap Singh vs. Savitri Pandey (2014) 10 SCC 713.

8. LIMITATION:
   The complaint is instituted within 30 days of the cause of action arising, strictly within the statutory limitation period under Section 142(1)(b) of the NI Act.

9. OFFENCE COMMITTED:
   That the acts of the Accused amount to an offence under Section 138 of the Negotiable Instruments Act, 1881, punishable with imprisonment up to 2 years, or fine up to twice the cheque amount, or both.

10. TERRITORIAL JURISDICTION (SECTION 142(2)(a) NI ACT):
    That this Hon'ble Court has exclusive territorial jurisdiction to entertain and try the present complaint as the cheque was presented for encashment at ${d.bank_name}${d.branch_name ? ' (' + d.branch_name + ' Branch)' : ''}, where the Complainant maintains an account, strictly in accordance with Section 142(2)(a) (as amended by NI Amendment Act, 2015) and Bridgestone India Pvt. Ltd. v. Inderpal Singh (2016) 2 SCC 341.${s141_averment}

PRAYER:
It is most respectfully prayed that this Hon'ble Court may be pleased to:

(a) Take cognizance of the offence under Section 138 of the NI Act;
(b) Issue summons/process against the Accused;
(c) Direct the Accused to pay Interim Compensation to the Complainant under Section 143A of the NI Act (up to 20% of the cheque amount);
(d) After trial, convict and sentence the Accused in accordance with law;
(e) Direct the Accused to pay fine/compensation of Rs.${(Number(d.cheque_amount) * 2).toLocaleString('en-IN')}/- (double the cheque amount) with interest;
(f) Pass any other order as deemed fit in the interest of justice.

AND FOR THIS ACT OF KINDNESS, THE COMPLAINANT AS IN DUTY BOUND SHALL EVER PRAY.

Date: ${formatDraftDate(d.filing_date)}
Place: _______________

                                        ________________________
                                        ${d.complainant_name}
                                        (Complainant)

VERIFICATION:
I, ${d.complainant_name}, the Complainant, do hereby solemnly affirm and state that the contents of paragraphs 1 to 10 of the above complaint are true and correct to the best of my knowledge, information and belief, and I believe the same to be true. No material fact has been concealed therefrom, and all supporting documents mentioned herein are annexed to the complaint.

Verified at _____________ on ${formatDraftDate(d.filing_date)}.

                                        ________________________
                                        (Complainant)
${bsaAnnexure}`;
            return timelineCheck.auditReport ? timelineCheck.auditReport + body : body;
        }
    },
    {
        id: 'affidavit_evidence', number: 4, title: 'Affidavit of Evidence', subtitle: 'Complainant',
        description: 'Sworn statement supporting the complaint, filed along with documents.',
        icon: 'fa-file-signature', color: '#f59e0b',
        fields: [
            { name: 'case_number', label: 'Case Number / CC No.', type: 'text', required: true, placeholder: 'e.g., CC/123/2024' },
            { name: 'court_name', label: 'Court Name', type: 'text', required: true },
            { name: 'complainant_name', label: 'Deponent / Complainant Name', type: 'text', required: true },
            { name: 'complainant_age', label: 'Age of Deponent', type: 'number', required: false, placeholder: 'e.g., 42' },
            { name: 'complainant_address', label: 'Deponent Address', type: 'textarea', required: true },
            { name: 'accused_name', label: 'Accused Name', type: 'text', required: true },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true },
            { name: 'cheque_date', label: 'Cheque Date', type: 'date', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (Rs.)', type: 'number', required: true },
            { name: 'bank_name', label: 'Bank Name', type: 'text', required: true },
            { name: 'dishonour_date', label: 'Dishonour Date', type: 'date', required: true },
            { name: 'dishonour_reason', label: 'Dishonour Reason', type: 'text', required: true },
            { name: 'notice_date', label: 'Demand Notice Date', type: 'date', required: true },
            { name: 'transaction_purpose', label: 'Underlying Debt/Transaction', type: 'textarea', required: true },
            { name: 'affidavit_date', label: 'Date of Affidavit', type: 'date', required: true }
        ],
        generate: function (d) {
            return `AFFIDAVIT OF EVIDENCE OF COMPLAINANT / PW-1

In the matter of: ${d.case_number}
IN THE COURT OF THE LEARNED MAGISTRATE AT ${d.court_name.toUpperCase()}

AFFIDAVIT OF ${d.complainant_name.toUpperCase()}
(Deponent / Complainant / PW-1)

-------------------------------------------------

I, ${d.complainant_name}${d.complainant_age ? ', aged about ' + d.complainant_age + ' years' : ''}, residing at ${d.complainant_address}, being the Complainant in the above case, do hereby solemnly affirm and state on oath as follows:

1. I am the Complainant in the above case and am fully conversant with the facts and circumstances of the case. I am competent to swear this affidavit.

2. That I advanced a sum of Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- to the Accused towards ${d.transaction_purpose}.

3. That in discharge of the legally enforceable debt/liability, the Accused issued Cheque No. ${d.cheque_number} dated ${formatDraftDate(d.cheque_date)} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/-, drawn on ${d.bank_name}. The original cheque is produced herewith as EXHIBIT-CW1/1.

4. That on presentation, the said cheque was returned/dishonoured on ${formatDraftDate(d.dishonour_date)} with the endorsement: "${d.dishonour_reason}." The original dishonour memo is produced herewith as EXHIBIT-CW1/2.

5. That I sent a statutory Legal Demand Notice to the Accused on ${formatDraftDate(d.notice_date)} by Registered Post/Speed Post. The office copy of notice, postal receipt and tracking report are produced herewith as EXHIBIT-CW1/3 (Colly).

6. That despite receipt of the notice, the Accused failed to make payment of the cheque amount within the statutory period of 15 days, thereby committing an offence under Section 138 of the NI Act.

7. I submit that all the documents/exhibits mentioned above are original and the contents of this affidavit are true to my knowledge.

8. I have not filed any other complaint or suit in respect of the same cause of action.

                                        DEPONENT
                                        ${d.complainant_name}

Verification:
Verified at ______________ on ${formatDraftDate(d.affidavit_date)} that the contents of this affidavit are true and correct.

                                        DEPONENT`;
        }
    },
    {
        id: 'condonation_delay', number: 7, title: 'Application – Condonation of Delay', subtitle: 'Late Filing',
        description: 'Filed when the complaint is submitted after the limitation period.',
        icon: 'fa-clock', color: '#f97316',
        fields: [
            { name: 'court_name', label: 'Court Name', type: 'text', required: true },
            { name: 'complainant_name', label: 'Complainant Name', type: 'text', required: true },
            { name: 'complainant_address', label: 'Complainant Address', type: 'textarea', required: true },
            { name: 'accused_name', label: 'Accused Name', type: 'text', required: true },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (Rs.)', type: 'number', required: true },
            { name: 'dishonour_date', label: 'Cheque Dishonour Date', type: 'date', required: true },
            { name: 'limitation_expiry', label: 'Limitation Period Expiry Date', type: 'date', required: true },
            { name: 'actual_filing_date', label: 'Actual Date of Filing', type: 'date', required: true },
            { name: 'delay_days', label: 'Approximate Days of Delay', type: 'number', required: true },
            { name: 'reason_for_delay', label: 'Reason for Delay (Detailed)', type: 'textarea', required: true, placeholder: 'Explain why the complaint could not be filed within limitation period...' }
        ],
        generate: function (d) {
            return `APPLICATION UNDER SECTION 142(b) OF THE NEGOTIABLE INSTRUMENTS ACT, 1881
READ WITH SECTION 5 OF THE LIMITATION ACT, 1963
FOR CONDONATION OF DELAY IN FILING COMPLAINT

IN THE ${d.court_name.toUpperCase()}

${d.complainant_name}                              ...Applicant/Complainant
VERSUS
${d.accused_name}                                  ...Respondent/Accused

-------------------------------------------------

APPLICATION FOR CONDONATION OF DELAY OF APPROXIMATELY ${d.delay_days} DAYS

-------------------------------------------------

MOST RESPECTFULLY SHEWETH:

1. That the Applicant/Complainant is filing the above complaint under Section 138 of the Negotiable Instruments Act, 1881, against the Respondent/Accused in relation to Cheque No. ${d.cheque_number} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- which was dishonoured on ${formatDraftDate(d.dishonour_date)}.

2. That the limitation period for filing the present complaint expired on ${formatDraftDate(d.limitation_expiry)}. The present complaint is being filed on ${formatDraftDate(d.actual_filing_date)}, i.e., a delay of approximately ${d.delay_days} days.

3. REASONS FOR DELAY:
${d.reason_for_delay}

4. That the delay in filing the complaint was not deliberate, wilful, or due to negligence on the part of the Complainant. The delay was caused due to the genuine and unavoidable reasons stated above.

5. That the Applicant submits that sufficient cause exists for condoning the said delay.

6. That no prejudice will be caused to the Respondent/Accused by condoning the said delay.

PRAYER:
It is most respectfully prayed that this Hon'ble Court may be pleased to condone the delay of ${d.delay_days} days in filing the above complaint and allow the complaint to be entertained on merits.

And for this act of kindness, the Applicant shall ever pray.

Date: ${formatDraftDate(d.actual_filing_date)}
Place: _______________

                                        ________________________
                                        ${d.complainant_name}
                                        (Applicant/Complainant)

VERIFICATION:
I, ${d.complainant_name}, verify that the contents of the above application are true and correct to the best of my knowledge and belief.

Verified at ______________ on ${formatDraftDate(d.actual_filing_date)}.`;
        }
    },
    {
        id: 'summons_application', number: 8, title: 'Summons / Process Application', subtitle: 'Court Summons',
        description: 'Request to the court to issue summons to the accused.',
        icon: 'fa-paper-plane', color: '#0891b2',
        fields: [
            { name: 'court_name', label: 'Court Name', type: 'text', required: true },
            { name: 'case_number', label: 'Case/CC Number', type: 'text', required: false },
            { name: 'complainant_name', label: 'Complainant Name', type: 'text', required: true },
            { name: 'accused_name', label: 'Accused Full Name', type: 'text', required: true },
            { name: 'accused_address', label: 'Accused Address (For Service)', type: 'textarea', required: true },
            { name: 'accused_phone', label: 'Accused Phone (if known)', type: 'tel', required: false },
            { name: 'accused_workplace', label: 'Accused Workplace Address (if known)', type: 'textarea', required: false },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (Rs.)', type: 'number', required: true },
            { name: 'mode_of_service', label: 'Preferred Mode of Summons Service', type: 'select', required: true, options: ['Ordinary Process', 'Registered Post', 'Registered Post + Ordinary Process', 'Police Service'] },
            { name: 'application_date', label: 'Date of Application', type: 'date', required: true }
        ],
        generate: function (d) {
            return `APPLICATION FOR ISSUANCE OF SUMMONS / PROCESS

IN THE ${d.court_name.toUpperCase()}
${d.case_number ? 'CASE NO.: ' + d.case_number : ''}

${d.complainant_name}                              ...Complainant
VERSUS
${d.accused_name}                                  ...Accused

-------------------------------------------------

APPLICATION UNDER SECTION 204 Cr.P.C. / BNSS FOR ISSUANCE OF PROCESS/SUMMONS

-------------------------------------------------

MOST RESPECTFULLY SHEWETH:

1. That the Complainant has filed a complaint under Section 138 of the Negotiable Instruments Act, 1881 against the Accused in the above case, relating to dishonour of Cheque No. ${d.cheque_number} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/-.

2. That it is necessary that summons/process be issued and served upon the Accused to ensure their appearance before this Hon'ble Court.

3. DETAILS OF ACCUSED FOR SERVICE OF SUMMONS:

   Accused Name    : ${d.accused_name}
   Address 1       : ${d.accused_address}
${d.accused_phone ? '   Contact No.     : ' + d.accused_phone : ''}
${d.accused_workplace ? '   Workplace Addr. : ' + d.accused_workplace : ''}

4. That the Complainant prays that summons be issued to the Accused by mode of ${d.mode_of_service} at the above address.

PRAYER:
It is most respectfully prayed that this Hon'ble Court may be pleased to:

(a) Issue summons/process against the Accused at the above address by ${d.mode_of_service};
(b) Fix a date for appearance of the Accused;
(c) Pass any other order as this Hon'ble Court deems fit.

Date: ${formatDraftDate(d.application_date)}
Place: _______________

                                        ________________________
                                        ${d.complainant_name}
                                        (Complainant)`;
        }
    },
    {
        id: 'notice_251', number: 9, title: 'Notice under Section 251 CrPC', subtitle: 'Charge Explanation',
        description: 'Document for reading and explaining charges to the accused in court.',
        icon: 'fa-balance-scale-right', color: '#7c3aed',
        fields: [
            { name: 'court_name', label: 'Court Name', type: 'text', required: true },
            { name: 'case_number', label: 'Case Number', type: 'text', required: true },
            { name: 'hearing_date', label: 'Date of Hearing', type: 'date', required: true },
            { name: 'complainant_name', label: 'Complainant Name', type: 'text', required: true },
            { name: 'accused_name', label: 'Accused Name', type: 'text', required: true },
            { name: 'accused_address', label: 'Accused Address', type: 'textarea', required: true },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true },
            { name: 'cheque_date', label: 'Cheque Date', type: 'date', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (Rs.)', type: 'number', required: true },
            { name: 'bank_name', label: 'Bank Name', type: 'text', required: true },
            { name: 'dishonour_date', label: 'Dishonour Date', type: 'date', required: true },
            { name: 'dishonour_reason', label: 'Dishonour Reason', type: 'text', required: true }
        ],
        generate: function (d) {
            return `NOTICE UNDER SECTION 251 OF THE CODE OF CRIMINAL PROCEDURE, 1973 / BNSS

IN THE ${d.court_name.toUpperCase()}

CASE NO.: ${d.case_number}

${d.complainant_name}                              ...Complainant
VERSUS
${d.accused_name}                                  ...Accused

-------------------------------------------------
NOTICE TO ACCUSED / PARTICULARS OF OFFENCE
-------------------------------------------------

Date: ${formatDraftDate(d.hearing_date)}

To,
${d.accused_name}
${d.accused_address}

TAKE NOTICE that on ${formatDraftDate(d.hearing_date)}, you appeared before this Court in the above case.

YOU ARE HEREBY INFORMED that the following charge/offence is alleged against you:

-------------------------------------------------
PARTICULARS OF THE OFFENCE:
-------------------------------------------------

That you, ${d.accused_name}, being the drawer of Cheque No. ${d.cheque_number} dated ${formatDraftDate(d.cheque_date)} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- (Rupees ${numberToWords(d.cheque_amount)} Only) drawn on ${d.bank_name}, issued the same in discharge of a legally enforceable debt/liability to the Complainant, ${d.complainant_name}.

That the said cheque was returned/dishonoured by the bank on ${formatDraftDate(d.dishonour_date)} with the reason: "${d.dishonour_reason}."

That despite receipt of a statutory Demand Notice, you failed to make the requisite payment within the stipulated period, thereby committing an offence punishable under:

SECTION 138 of the Negotiable Instruments Act, 1881
(Dishonour of cheque for insufficiency, etc. of funds in the account)

-------------------------------------------------

DO YOU PLEAD GUILTY OR NOT GUILTY?

[ ] I PLEAD GUILTY to the charge as stated above.
[ ] I PLEAD NOT GUILTY and claim to be tried.

-------------------------------------------------

Reply of Accused: ______________________________________

Signature of Accused: _________________________
Date: ${formatDraftDate(d.hearing_date)}

                                        JUDICIAL MAGISTRATE
                                        ${d.court_name}`;
        }
    },
    {
        id: 'cross_examination', number: 10, title: 'Cross-Examination Questions Draft', subtitle: 'Trial Strategy',
        description: 'Prepared questions for cross-examining the accused or complainant.',
        icon: 'fa-comments', color: '#dc2626',
        fields: [
            { name: 'case_number', label: 'Case Number', type: 'text', required: false },
            { name: 'court_name', label: 'Court Name', type: 'text', required: true },
            { name: 'witness_name', label: 'Name of Witness Being Cross-Examined', type: 'text', required: true },
            { name: 'witness_role', label: 'Role of Witness', type: 'select', required: true, options: ['PW-1 (Complainant)', 'DW-1 (Accused)', 'PW-2 (Bank Official)', 'Other Prosecution Witness', 'Other Defence Witness'] },
            { name: 'complainant_name', label: 'Complainant Name', type: 'text', required: true },
            { name: 'accused_name', label: 'Accused Name', type: 'text', required: true },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (Rs.)', type: 'number', required: true },
            { name: 'defence_strategy', label: 'Cross Strategy Focus', type: 'select', required: true, options: ['Challenge debt existence', 'Challenge notice validity', 'Challenge cheque issuance purpose', 'Challenge bank memo authenticity', 'Challenge limitation period', 'General credibility attack'] },
            { name: 'cross_date', label: 'Date of Cross-Examination', type: 'date', required: false }
        ],
        generate: function (d) {
            const strategyQs = {
                'Challenge debt existence': `Q17. (*) You have no written agreement, loan deed, or promissory note to prove the alleged debt?
Q18. The money was allegedly given in cash - do you have any receipt or record of payment?
Q19. Is it possible that the amount was a gift or advance and not a loan?
Q20. You cannot produce any bank statement showing the transfer of Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- to the Accused?
Q21. The entire foundation of your complaint rests solely on a self-serving oral statement?`,
                'Challenge notice validity': `Q17. (*) Can you produce the original postal receipt proving the notice was dispatched within 30 days of dishonour?
Q18. The A.D. card - is the handwriting on it that of the Accused?
Q19. You admitted the notice was sent to an address which was not the Accused's last known address?
Q20. Is it correct that the notice did not specifically demand the exact cheque amount?
Q21. (*) The 15-day period for payment had not yet expired when you filed this complaint?`,
                'Challenge cheque issuance purpose': `Q17. (*) Is it not correct that the cheque was given as a blank security cheque?
Q18. You are aware that security cheques cannot be the subject matter of Section 138 proceedings?
Q19. When exactly did you fill in the date and amount on the cheque?
Q20. Do you have any document showing the cheque was issued as repayment and not as security?
Q21. The Accused had specifically asked you to return the cheque when the purpose was served?`,
                'Challenge bank memo authenticity': `Q17. Can you identify the bank official who signed the dishonour memo (Exh. B)?
Q18. Have you ever produced that official as a witness before this court?
Q19. (*) The dishonour memo has no seal of the bank - is that correct?
Q20. Are you aware that bank records can be altered?
Q21. The bank account from which the cheque was drawn was in fact active at the material time?`,
                'Challenge limitation period': `Q17. When exactly did you first receive the bank dishonour memo?
Q18. (*) So the notice was sent more than 30 days after the dishonour, is that correct?
Q19. When was the complaint actually filed before this court?
Q20. You are aware that if the complaint is filed beyond one month from the cause of action, it is barred by limitation?
Q21. No application for condonation of delay was filed before this court?`,
                'General credibility attack': `Q17. You have a financial dispute with the Accused on matters unrelated to this cheque?
Q18. (*) You have filed / threatened to file multiple cases against the Accused?
Q19. The entire case is based on your own self-interested testimony without independent corroboration?
Q20. You stand to receive Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- if the Accused is convicted?
Q21. This complaint is a pressure tactic to settle a separate civil dispute?`
            };
            return `DRAFT QUESTIONS FOR CROSS-EXAMINATION OF ${d.witness_role.toUpperCase()}

CASE: ${d.case_number || '_____________'}
COURT: ${d.court_name}
${d.complainant_name} vs. ${d.accused_name}

WITNESS: ${d.witness_name} (${d.witness_role})
STRATEGY FOCUS: ${d.defence_strategy}
${d.cross_date ? 'DATE: ' + formatDraftDate(d.cross_date) : ''}

-------------------------------------------------
DRAFT CROSS-EXAMINATION QUESTIONS
-------------------------------------------------

SECTION A - PRELIMINARY / BACKGROUND QUESTIONS

Q1.  You are aware of the present case being CC No. ${d.case_number || '_____'} pending before this court?
Q2.  You have given your examination-in-chief by way of affidavit before this court, is that correct?
Q3.  You are personally acquainted with both the Complainant and the Accused?
Q4.  How long have you known the Complainant / Accused?

-------------------------------------------------
SECTION B - ON CHEQUE & TRANSACTION

Q5.  The cheque bearing No. ${d.cheque_number} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- was allegedly drawn in discharge of a legally enforceable debt - is that your claim?
Q6.  Do you have any written agreement, promissory note, or document evidencing the loan or debt?
Q7.  Is it correct that no written agreement was executed between the parties?
Q8.  Was the cheque handed over in the presence of any witness?
Q9.  Is it possible that the cheque was given as security and not towards repayment of debt?
Q10. Are you aware that a post-dated / security cheque cannot form the basis of Section 138 proceedings?

-------------------------------------------------
SECTION C - ON NOTICE & SERVICE

Q11. You claim to have sent a legal notice under Section 138 - when was it sent?
Q12. By what mode was the notice sent?
Q13. Do you have the original postal receipt showing the notice was dispatched?
Q14. Was the A.D. card returned? Is the signature on the A.D. card that of the Accused?
Q15. Is it possible that the notice was sent to an incorrect address?
Q16. You have no proof that the Accused actually received the notice, correct?

-------------------------------------------------
SECTION D - STRATEGY: ${d.defence_strategy}

${strategyQs[d.defence_strategy] || 'Q17. Are all the facts stated in your examination-in-chief true?\nQ18. Is there any fact you may have omitted or exaggerated?\nQ19. You have no independent witness to corroborate your claims?\nQ20. Is it possible your recollection of dates and amounts may be incorrect?'}

-------------------------------------------------
SECTION E - CLOSING / CREDIBILITY

Q25. You stand to gain financially if the Accused is convicted, correct?
Q26. Is it possible that the entire complaint is filed for harassment?
Q27. You are relying entirely on what you have been told by your lawyer?

-------------------------------------------------
NOTE TO ADVOCATE:
- (*) marks high-impact questions. Use them strategically.
- Adapt to actual evidence and admissions in examination-in-chief.
- Do not ask questions the answers to which you cannot control.
- Stop cross-examination once a favorable admission is obtained.`;
        }
    },
    {
        id: 'final_arguments', number: 11, title: 'Final Arguments (Written Submissions)', subtitle: 'Legal Reasoning',
        description: 'Legal reasoning, case law citations, and conclusion for final arguments.',
        icon: 'fa-scroll', color: '#065f46',
        fields: [
            { name: 'court_name', label: 'Court Name', type: 'text', required: true },
            { name: 'case_number', label: 'Case Number', type: 'text', required: true },
            { name: 'complainant_name', label: 'Complainant Name', type: 'text', required: true },
            { name: 'accused_name', label: 'Accused Name', type: 'text', required: true },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true },
            { name: 'cheque_amount', label: 'Cheque Amount (Rs.)', type: 'number', required: true },
            { name: 'cheque_date', label: 'Cheque Date', type: 'date', required: true },
            { name: 'dishonour_date', label: 'Dishonour Date', type: 'date', required: true },
            { name: 'dishonour_reason', label: 'Dishonour Reason', type: 'text', required: true },
            { name: 'side', label: 'Arguments Filed By', type: 'select', required: true, options: ['Complainant Side', 'Accused Side / Defence'] },
            { name: 'key_facts', label: 'Key Facts Proved During Trial', type: 'textarea', required: true, placeholder: 'Summarise key facts proved through evidence...' },
            { name: 'arguments_date', label: 'Date of Filing', type: 'date', required: true }
        ],
        generate: function (d) {
            const isComp = d.side === 'Complainant Side';
            return `WRITTEN ARGUMENTS / FINAL SUBMISSIONS

IN THE ${d.court_name.toUpperCase()}

CASE NO.: ${d.case_number}

${d.complainant_name}                              ...Complainant
VERSUS
${d.accused_name}                                  ...Accused

WRITTEN ARGUMENTS ON BEHALF OF THE ${d.side.toUpperCase()}

DATE: ${formatDraftDate(d.arguments_date)}

-------------------------------------------------

I. INTRODUCTION

The present complaint has been filed under Section 138 of the Negotiable Instruments Act, 1881 by the Complainant, ${d.complainant_name}, against the Accused, ${d.accused_name}, on account of dishonour of Cheque No. ${d.cheque_number} dated ${formatDraftDate(d.cheque_date)} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- with the endorsement "${d.dishonour_reason}" on ${formatDraftDate(d.dishonour_date)}.

-------------------------------------------------

II. FACTS OF THE CASE

${d.key_facts}

-------------------------------------------------

III. INGREDIENTS OF SECTION 138 NI ACT

A. EXISTENCE OF LEGALLY ENFORCEABLE DEBT OR LIABILITY:
   ${isComp ? 'The Complainant has conclusively proved through documents and testimony that the cheque was issued towards repayment of a lawful debt.' : 'The Defence contends that no legally enforceable debt or liability existed. The cheque was issued as security / under duress / without consideration.'}

B. DISHONOUR OF CHEQUE:
   Cheque No. ${d.cheque_number} was returned/dishonoured on ${formatDraftDate(d.dishonour_date)} with the memo "${d.dishonour_reason}." This has been proved through the original dishonour memo (Exh. B).

C. STATUTORY DEMAND NOTICE:
   ${isComp ? 'A valid demand notice was sent by Registered Post within 30 days of dishonour. The Accused failed to pay within the statutory period of 15 days from receipt of notice.' : 'The Defence submits that the demand notice was not validly served / was sent beyond the 30-day statutory period / did not contain the requisite demand.'}

D. FAILURE TO MAKE PAYMENT:
   ${isComp ? 'The Accused failed and neglected to make payment of Rs.' + Number(d.cheque_amount).toLocaleString('en-IN') + '/- within 15 days of receipt of notice, thereby completing the offence.' : 'The Accused had no liability to pay as the alleged debt was non-existent / already repaid / the cheque was misused.'}

-------------------------------------------------

IV. RELEVANT CASE LAW

1. Dashrath Rupsingh Rathod v. State of Maharashtra (2014) 9 SCC 129
   (Jurisdiction of court where complaint is to be filed)

2. Meters and Instruments Pvt. Ltd. v. Kanchan Mehta (2018) 1 SCC 560
   (Presumption under Sec. 139; summary trial powers)

3. M.S. Narayana Menon v. State of Kerala (2006) 6 SCC 39
   (Burden of proof and presumption under Section 139 NI Act)

4. K. Bhaskaran v. Sankaran Vaidhyan Balan (1999) 7 SCC 510
   (Five ingredients of Sec. 138; place of filing of complaint)

5. Kusum Ingots & Alloys Ltd. v. Pennor India (P) Ltd (2000) 2 SCC 745
   (Vicarious liability of directors under Section 141 NI Act)

-------------------------------------------------

V. CONCLUSION & PRAYER

${isComp ?
                    `The Complainant has proved all the ingredients of Section 138 NI Act beyond reasonable doubt. The Accused is guilty of the offence charged.

PRAYER: The Accused may be convicted under Section 138 of the NI Act and sentenced to appropriate imprisonment and/or directed to pay compensation of Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/- to the Complainant with interest, in the interest of justice.` :
                    `The prosecution has failed to prove the essential ingredients of Section 138 NI Act beyond reasonable doubt. The Accused is entitled to be acquitted.

PRAYER: The Accused may be acquitted of the charge under Section 138 NI Act, as the prosecution has failed to establish its case beyond reasonable doubt.`}

Date: ${formatDraftDate(d.arguments_date)}
Place: _______________

                                        Respectfully submitted,
                                        Advocate for the ${d.side}`;
        }
    },
    {
        id: 'settlement_agreement', number: 12, title: 'Compounding / Settlement Agreement', subtitle: 'Case Closure',
        description: 'Used when parties settle. Ends litigation under Section 147 NI Act.',
        icon: 'fa-handshake', color: '#059669',
        fields: [
            { name: 'court_name', label: 'Court Name', type: 'text', required: true },
            { name: 'case_number', label: 'Case Number', type: 'text', required: true },
            { name: 'complainant_name', label: 'Complainant Full Name', type: 'text', required: true },
            { name: 'complainant_address', label: 'Complainant Address', type: 'textarea', required: true },
            { name: 'accused_name', label: 'Accused Full Name', type: 'text', required: true },
            { name: 'accused_address', label: 'Accused Address', type: 'textarea', required: true },
            { name: 'cheque_number', label: 'Cheque Number', type: 'text', required: true },
            { name: 'cheque_amount', label: 'Original Cheque Amount (Rs.)', type: 'number', required: true },
            { name: 'settlement_amount', label: 'Agreed Settlement Amount (Rs.)', type: 'number', required: true },
            { name: 'payment_mode', label: 'Mode of Settlement Payment', type: 'select', required: true, options: ['Full upfront payment', 'Instalments (as per schedule)', 'Cheque/DD', 'Online Transfer (NEFT/RTGS)', 'Cash (with receipt)'] },
            { name: 'payment_date', label: 'Date of Payment / First Instalment', type: 'date', required: true },
            { name: 'settlement_date', label: 'Date of This Agreement', type: 'date', required: true },
            { name: 'instalment_details', label: 'Instalment Schedule (if applicable)', type: 'textarea', required: false, placeholder: 'e.g., Rs.50,000 on 01-02-2025, Rs.50,000 on 01-03-2025...' }
        ],
        generate: function (d) {
            return `COMPOUNDING / SETTLEMENT AGREEMENT
UNDER SECTION 147 OF THE NEGOTIABLE INSTRUMENTS ACT, 1881

IN THE ${d.court_name.toUpperCase()}

CASE NO.: ${d.case_number}

${d.complainant_name}                              ...Complainant
VERSUS
${d.accused_name}                                  ...Accused

-------------------------------------------------

This Compounding Agreement is executed on ${formatDraftDate(d.settlement_date)}, between:

PARTY 1 (Complainant):
${d.complainant_name}, residing at ${d.complainant_address} (hereinafter "Complainant")

AND

PARTY 2 (Accused):
${d.accused_name}, residing at ${d.accused_address} (hereinafter "Accused")

WHEREAS:

A. The Complainant had filed a complaint under Section 138 of the Negotiable Instruments Act, 1881, being Case No. ${d.case_number}, before the ${d.court_name}, in relation to dishonour of Cheque No. ${d.cheque_number} for Rs.${Number(d.cheque_amount).toLocaleString('en-IN')}/-.

B. The parties have now arrived at a mutually agreeable full and final settlement of the above dispute without admission of any liability by either party.

NOW, THEREFORE, the parties agree as follows:

1. SETTLEMENT AMOUNT: The Accused agrees to pay and the Complainant agrees to accept a sum of Rs.${Number(d.settlement_amount).toLocaleString('en-IN')}/- (Rupees ${numberToWords(d.settlement_amount)} Only) as full and final settlement of all claims arising out of the above complaint and the underlying dispute.

2. MODE OF PAYMENT: ${d.payment_mode}

3. DATE OF PAYMENT: ${formatDraftDate(d.payment_date)}
${d.instalment_details ? '\n4. INSTALMENT SCHEDULE:\n' + d.instalment_details : ''}

4. NO FURTHER CLAIMS: Upon receipt of the settlement amount, the Complainant agrees that all claims, counter-claims, disputes, and proceedings in relation to the said cheque and underlying transaction shall stand fully and finally settled. The Complainant shall not initiate any fresh proceedings in relation to the same cause of action.

5. WITHDRAWAL OF COMPLAINT: The Complainant agrees to file a Joint Compounding Application before the ${d.court_name} and take all necessary steps for quashing/withdrawal of the above complaint.

6. RETURN OF CHEQUE: Upon receipt of settlement, the Complainant shall return the original cheque to the Accused.

SIGNATURES:

____________________________          ____________________________
${d.complainant_name}                 ${d.accused_name}
(Complainant)                         (Accused)

Date: ${formatDraftDate(d.settlement_date)}

-------------------------------------------------

JOINT APPLICATION FOR COMPOUNDING BEFORE COURT

To,
The Ld. Judicial Magistrate,
${d.court_name}

Sub: Joint Application for Compounding of Offence under Section 147 of the NI Act in Case No. ${d.case_number}

Respectfully Sheweth:

That the Complainant and the Accused have amicably settled the above dispute and the accused has agreed to pay Rs.${Number(d.settlement_amount).toLocaleString('en-IN')}/- as full and final settlement. Both parties jointly pray that:

(a) The offence be compounded under Section 147 of the NI Act;
(b) The complaint be dismissed as withdrawn / settled;
(c) Accused be acquitted on the basis of this compounding.

                    Complainant: ${d.complainant_name}
                    Accused: ${d.accused_name}

Date: ${formatDraftDate(d.settlement_date)}`;
        }
    }
];

// ---------------- Template Helper Functions ---------------------------------

function formatDraftDate(dateStr) {
    if (!dateStr) return '_______________';
    try {
        const d = new Date(dateStr);
        if (isNaN(d.getTime())) return dateStr;
        return d.toLocaleDateString('en-IN', { day: '2-digit', month: 'long', year: 'numeric' });
    } catch (e) { return dateStr; }
}

function numberToWords(num) {
    if (!num || isNaN(num) || num == 0) return 'zero';

    const ones = ['', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten', 'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen', 'sixteen', 'seventeen', 'eighteen', 'nineteen'];
    const tens = ['', '', 'twenty', 'thirty', 'forty', 'fifty', 'sixty', 'seventy', 'eighty', 'ninety'];

    function convert_less_than_thousand(n) {
        if (n === 0) return '';
        if (n < 20) return ones[n];
        if (n < 100) return tens[Math.floor(n / 10)] + (n % 10 !== 0 ? '-' + ones[n % 10] : '');
        return ones[Math.floor(n / 100)] + ' hundred' + (n % 100 !== 0 ? ' and ' + convert_less_than_thousand(n % 100) : '');
    }

    let n = Math.floor(num);
    let res = '';

    if (n >= 10000000) {
        res += convert_less_than_thousand(Math.floor(n / 10000000)) + ' crore ';
        n %= 10000000;
    }
    if (n >= 100000) {
        res += convert_less_than_thousand(Math.floor(n / 100000)) + ' lakh ';
        n %= 100000;
    }
    if (n >= 1000) {
        res += convert_less_than_thousand(Math.floor(n / 1000)) + ' thousand ';
        n %= 1000;
    }
    if (n > 0) {
        res += convert_less_than_thousand(n);
    }

    return res.trim().toUpperCase();
}

// ---------------- Screen & UI Functions -------------------------------------


export { DRAFT_TYPES, formatDraftDate, numberToWords };
