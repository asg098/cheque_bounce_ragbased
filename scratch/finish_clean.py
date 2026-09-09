import re, sys, io
if isinstance(sys.stdout, io.TextIOWrapper):
    sys.stdout.reconfigure(encoding='utf-8')


def finish_cleaning():
    with open('frontend/index.html', 'r', encoding='utf-8') as f:
        html = f.read()

    # 1. Mobile nav bank recovery button
    html = re.sub(
        r'<button class="btn btn-outline u-is-10"[^>]*>.*?</button>',
        '',
        html,
        flags=re.DOTALL
    )

    # 2. Registration domain picker
    old_picker_pattern = r'<div class="domain-picker u-is-42" id="domainPickerGrid">.*?</div>\s*<div class="domain-picker-error"'
    new_picker_replacement = '''<div class="domain-picker u-is-42" id="domainPickerGrid" style="grid-template-columns: 1fr;">
                                <div class="domain-card selected--ni" id="domainCard_ni" onclick="selectRegisterDomain('ni_act')">
                                    <span class="dc-check" id="domainCheck_ni" style="display:inline-block;"><i class="fas fa-check"></i></span>
                                    <span class="dc-icon">⚖️</span>
                                    <span class="dc-title">NI Act — Section 138 Litigation Intelligence</span>
                                    <span class="dc-sub">Cheque Dishonour, Statutory Limitation, S.139 Presumptions, S.141 Vicarious Liability &amp; S.143A Interim Compensation</span>
                                </div>
                            </div>
                            <div class="domain-picker-error"'''
    html = re.sub(old_picker_pattern, new_picker_replacement, html, flags=re.DOTALL)

    # 3. Remove bankAuthModal and createBankOfficerModal
    # They start at <!-- Bank Officer Authentication & Registration Modal --> and end before <!-- ═══════════════════════════════════════════════════════════════ -->\n    <!-- MODAL: SAVE CASE VERSION SNAPSHOT -->
    modal_start = html.find('<!-- Bank Officer Authentication & Registration Modal -->')
    modal_end = html.find('<!-- MODAL: SAVE CASE VERSION SNAPSHOT -->')
    if modal_start != -1 and modal_end != -1:
        # find the banner line right before saveVersionModal
        banner_start = html.rfind('<!-- ═══════════════════════════════════════════════════════════════ -->', 0, modal_end)
        html = html[:modal_start] + html[banner_start:]
        print("Removed bankAuthModal and createBankOfficerModal")
    else:
        print("Bank modals not found:", modal_start, modal_end)

    # 4. Terms modal point 2 list
    terms_pattern = r'<li><strong>Section 138/141/142 Negotiable Instruments Act, 1881:</strong>.*?</ul>'
    terms_replacement = '<li><strong>Section 138/141/142 Negotiable Instruments Act, 1881:</strong> Limitation computing, cheque dishonour tracking, statutory notice verification, director vicarious liability, and Section 143A interim compensation modeling.</li>\n                    </ul>'
    html = re.sub(terms_pattern, terms_replacement, html, flags=re.DOTALL)

    with open('frontend/index.html', 'w', encoding='utf-8') as f:
        f.write(html)

    print("finish_cleaning completed")

if __name__ == '__main__':
    finish_cleaning()
