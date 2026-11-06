/* Shared, progressively enhanced reading UI. All chapter content is static HTML. */
(() => {
  document.body.classList.add('js');
  document.getElementById('menu-toggle')?.addEventListener('click', (event) => {
    const open = document.body.classList.toggle('nav-open');
    event.currentTarget.setAttribute('aria-expanded', String(open));
  });
  document.getElementById('chapter-filter')?.addEventListener('input', (event) => {
    const needle = event.target.value.trim().toLocaleLowerCase();
    document.querySelectorAll('#chapter-nav nav li').forEach((li) => {
      li.hidden = !li.textContent.toLocaleLowerCase().includes(needle);
    });
  });
  document.querySelectorAll('.code-block').forEach((block) => {
    const button = block.querySelector('.copy-code');
    const code = block.querySelector('code');
    block.querySelector('pre')?.setAttribute('tabindex', '0');
    button?.addEventListener('click', async () => {
      try {
        if (!navigator.clipboard) throw new Error('Clipboard unavailable');
        await navigator.clipboard.writeText(code.textContent);
        button.textContent = 'Copied';
      } catch (_) {
        const range = document.createRange();
        range.selectNodeContents(code);
        const selection = window.getSelection();
        selection.removeAllRanges(); selection.addRange(range);
        button.textContent = 'Selected — copy manually';
      }
      window.setTimeout(() => { button.textContent = 'Copy'; }, 2200);
    });
  });
  document.querySelectorAll('article table').forEach((table) => {
    const wrapper = document.createElement('div'); wrapper.className = 'table-wrap';
    table.replaceWith(wrapper); wrapper.append(table);
  });
  const equations = document.querySelectorAll('.math-input');
  async function renderEquations() {
    if (!equations.length) return;
    if (!window.MathJax?.startup?.promise) {
      document.getElementById('math-status').hidden = false;
      return;
    }
    await window.MathJax.startup.promise;
    for (const node of equations) {
      try {
        const output = await window.MathJax.tex2svgPromise(node.textContent, {
          display: node.dataset.display === 'true'
        });
        if (output.querySelector('[data-mjx-error]')) throw new Error('Invalid TeX');
        node.replaceChildren(output);
        node.classList.add('math-rendered');
      } catch (_) {
        document.getElementById('math-status').hidden = false;
      }
    }
    // Standalone conversions need the output/accessibility styles installed too.
    // Keep assistive MathML available without displaying a duplicate equation.
    window.MathJax.startup.document.clear();
    window.MathJax.startup.document.updateDocument();
  }
  renderEquations().catch(() => {
    document.getElementById('math-status').hidden = false;
  });
})();
