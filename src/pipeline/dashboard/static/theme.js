(() => {
  const STORAGE_KEY = "prague-weather-theme";
  const THEME_LABELS = {
    light: "Light",
    dark: "Dark",
  };

  function readQueryTheme() {
    const params = new URLSearchParams(window.location.search);
    const queryTheme = params.get("theme");
    if (queryTheme === "light" || queryTheme === "dark") {
      return queryTheme;
    }

    return null;
  }

  function readStoredTheme() {
    try {
      const storedTheme = window.localStorage.getItem(STORAGE_KEY);
      if (storedTheme === "light" || storedTheme === "dark") {
        return storedTheme;
      }
    } catch (error) {
      return null;
    }

    return null;
  }

  function getSystemTheme() {
    if (
      window.matchMedia &&
      window.matchMedia("(prefers-color-scheme: dark)").matches
    ) {
      return "dark";
    }

    return "light";
  }

  function applyTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
  }

  function updateToggle(theme) {
    const toggle = document.querySelector("[data-theme-toggle]");
    const label = document.querySelector("[data-theme-label]");

    if (!toggle || !label) {
      return;
    }

    label.textContent = THEME_LABELS[theme];
    toggle.setAttribute("aria-pressed", String(theme === "dark"));
    toggle.setAttribute("data-active-theme", theme);
  }

  function persistTheme(theme) {
    try {
      window.localStorage.setItem(STORAGE_KEY, theme);
    } catch (error) {
      return;
    }
  }

  let activeTheme = readQueryTheme() || readStoredTheme() || getSystemTheme();
  applyTheme(activeTheme);

  document.addEventListener("DOMContentLoaded", () => {
    updateToggle(activeTheme);

    const toggle = document.querySelector("[data-theme-toggle]");
    if (!toggle) {
      return;
    }

    toggle.addEventListener("click", () => {
      activeTheme = activeTheme === "dark" ? "light" : "dark";
      applyTheme(activeTheme);
      updateToggle(activeTheme);
      persistTheme(activeTheme);
    });
  });
})();
