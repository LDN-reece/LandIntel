document.addEventListener("DOMContentLoaded", () => {
  const storage = window.localStorage;

  const detailsNodes = document.querySelectorAll("details[data-persist-id]");
  detailsNodes.forEach((node) => {
    const key = `landintel:details:${node.dataset.persistId}`;
    const saved = storage.getItem(key);

    if (saved !== null) {
      node.open = saved === "true";
    }

    node.addEventListener("toggle", () => {
      storage.setItem(key, String(node.open));
    });
  });

  const toggleGroups = document.querySelectorAll("[data-layer-toggle-group]");
  toggleGroups.forEach((group) => {
    const groupId = group.dataset.layerToggleGroup;
    const buttons = Array.from(group.querySelectorAll("[data-layer-target]"));
    const panels = Array.from(document.querySelectorAll(`[data-layer-group="${groupId}"][data-layer-panel]`));

    if (!buttons.length || !panels.length) {
      return;
    }

    const storageKey = `landintel:layer:${groupId}`;
    const enabledTargets = buttons
      .filter((button) => button.dataset.layerEnabled !== "false")
      .map((button) => button.dataset.layerTarget);

    const setActiveTarget = (target) => {
      buttons.forEach((button) => {
        const isActive = button.dataset.layerTarget === target;
        button.setAttribute("aria-pressed", String(isActive));
      });

      panels.forEach((panel) => {
        const isVisible = panel.dataset.layerPanel === target;
        panel.dataset.layerVisible = String(isVisible);
      });

      storage.setItem(storageKey, target);
    };

    const savedTarget = storage.getItem(storageKey);
    const initialTarget = enabledTargets.includes(savedTarget) ? savedTarget : enabledTargets[0];

    if (initialTarget) {
      setActiveTarget(initialTarget);
    }

    buttons.forEach((button) => {
      if (button.dataset.layerEnabled === "false") {
        button.setAttribute("aria-pressed", "false");
        return;
      }

      button.addEventListener("click", () => {
        setActiveTarget(button.dataset.layerTarget);
      });
    });
  });

  const updateScrollState = () => {
    document.body.classList.toggle("is-scrolled", window.scrollY > 12);
  };

  updateScrollState();
  window.addEventListener("scroll", updateScrollState, { passive: true });
});
