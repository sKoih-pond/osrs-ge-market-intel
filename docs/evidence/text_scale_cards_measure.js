// Overflow check for the GE screener's idea cards under enlarged text.
//
// Open /ge/ (a local copy of site/src with a snapshot, or the live page) in a browser with
// mobile emulation at 375x812 or 320x640, paste this file into the console, then run
// __sweep(375) or __sweep(320). Each row sets the root font size (100% to 200% in 10%
// steps, which is what a larger browser text size does to this rem-based page) and reports:
//   docWidth / sidewaysPx  document width against the intended viewport width W. W is a
//                          constant on purpose: a mobile viewport widens itself to fit
//                          overflowing content, so comparing with innerWidth passes a
//                          broken page.
//   pagePastEdgePx         any text or box past 0..W, outside the scrollable table region.
//   cardsOverflowing       idea cards with any text run or box past the card's content edge.
//   splitNumbers           numbers (digits with thousands separators) wrapped across lines.
// __fp(scale) returns a hash of every element box, to show a layout did not move; take it
// at the top of the page, since it uses viewport coordinates.
(() => {
  const edge = (r, left, right) => Math.max(r.right - right, left - r.left);

  function scan(root, left, right, skip) {
    let over = 0;
    let what = "";
    const w = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    for (let n; (n = w.nextNode()); ) {
      if (skip(n.parentElement) || !n.textContent.trim()) continue;
      const range = document.createRange();
      range.selectNodeContents(n);
      for (const t of range.getClientRects()) {
        const o = edge(t, left, right);
        if (o > over) { over = o; what = n.textContent.trim().slice(0, 40); }
      }
    }
    for (const e of root.querySelectorAll("*")) {
      if (skip(e)) continue;
      const b = e.getBoundingClientRect();
      if (!b.width && !b.height) continue;
      const o = edge(b, left, right);
      if (o > over) { over = o; what = `<${e.tagName.toLowerCase()}> ${e.textContent.trim().slice(0, 30)}`; }
    }
    return { over, what };
  }

  function splitNumbers() {
    const found = [];
    for (const c of document.querySelectorAll(".idea")) {
      const w = document.createTreeWalker(c, NodeFilter.SHOW_TEXT);
      for (let n; (n = w.nextNode()); ) {
        for (const m of n.textContent.matchAll(/\d[\d,]*\d/g)) {
          const r = document.createRange();
          r.setStart(n, m.index);
          r.setEnd(n, m.index + m[0].length);
          const tops = new Set([...r.getClientRects()].map((t) => Math.round(t.top)));
          if (tops.size > 1) found.push(`${c.querySelector("h3").textContent}: ${m[0]}`);
        }
      }
    }
    return found;
  }

  window.__measure = function (scale, W) {
    document.documentElement.style.fontSize = scale;
    const page = scan(document.body, 0, W, (e) => !!e && !!e.closest(".scroll, .skip, noscript"));
    const cards = [...document.querySelectorAll(".idea")];
    let overflowing = 0;
    let worst = 0;
    let example = "";
    for (const c of cards) {
      const cs = getComputedStyle(c);
      const r = c.getBoundingClientRect();
      const left = r.left + parseFloat(cs.borderLeftWidth) + parseFloat(cs.paddingLeft);
      const right = r.right - parseFloat(cs.borderRightWidth) - parseFloat(cs.paddingRight);
      const { over, what } = scan(c, left, right, () => false);
      if (over > 0.5) {
        overflowing++;
        if (over > worst) { worst = over; example = `${c.querySelector("h3").textContent}: "${what}"`; }
      }
    }
    const split = splitNumbers();
    return {
      scale,
      docWidth: document.documentElement.scrollWidth,
      sidewaysPx: document.documentElement.scrollWidth - W,
      pagePastEdgePx: Math.round(Math.max(0, page.over)),
      pageExample: page.over > 0.5 ? page.what : "",
      cards: cards.length,
      cardsOverflowing: overflowing,
      worstCardPx: Math.round(worst),
      cardExample: example,
      splitNumbers: split.length,
      splitExample: split[0] || "",
    };
  };

  window.__sweep = function (W) {
    const out = [];
    for (let p = 100; p <= 200; p += 10) out.push(window.__measure(`${p}%`, W));
    return out;
  };

  window.__fp = function (scale) {
    document.documentElement.style.fontSize = scale;
    const t = [...document.querySelectorAll("body *")]
      .filter((e) => !e.closest("noscript"))
      .map((e) => {
        const b = e.getBoundingClientRect();
        return [b.left, b.top, b.width, b.height].map(Math.round).join(",");
      })
      .join(";");
    let h = 0;
    for (const ch of t) h = (h * 31 + ch.charCodeAt(0)) | 0;
    return h;
  };
})();
