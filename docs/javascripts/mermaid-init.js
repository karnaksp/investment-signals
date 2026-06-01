(function () {
  function currentTheme() {
    var scheme =
      document.body.getAttribute("data-md-color-scheme") ||
      document.documentElement.getAttribute("data-md-color-scheme");
    return scheme === "slate" ? "dark" : "default";
  }

  function sourceFor(block) {
    if (!block.dataset.mermaidSource) {
      block.dataset.mermaidSource = block.textContent.trim();
    }
    return block.dataset.mermaidSource;
  }

  function resetRenderedBlocks() {
    document.querySelectorAll(".mermaid").forEach(function (block) {
      block.removeAttribute("data-processed");
      block.textContent = sourceFor(block);
    });
  }

  function configureMermaid() {
    if (!window.mermaid) {
      return false;
    }

    window.mermaid.initialize({
      startOnLoad: false,
      securityLevel: "loose",
      theme: currentTheme()
    });
    return true;
  }

  function renderMermaid() {
    if (!configureMermaid()) {
      return;
    }

    var blocks = Array.from(document.querySelectorAll(".mermaid"));
    if (!blocks.length) {
      return;
    }

    blocks.forEach(function (block) {
      sourceFor(block);
    });

    window.mermaid.run({ nodes: blocks }).catch(function (error) {
      console.error("Mermaid render failed", error);
    });
  }

  if (window.document$ && typeof window.document$.subscribe === "function") {
    window.document$.subscribe(renderMermaid);
  } else {
    document.addEventListener("DOMContentLoaded", renderMermaid);
  }

  document.addEventListener("DOMContentLoaded", function () {
    var target = document.body || document.documentElement;
    new MutationObserver(function (mutations) {
      var changed = mutations.some(function (mutation) {
        return mutation.attributeName === "data-md-color-scheme";
      });
      if (changed) {
        resetRenderedBlocks();
        renderMermaid();
      }
    }).observe(target, {
      attributes: true,
      attributeFilter: ["data-md-color-scheme"]
    });
  });
})();
