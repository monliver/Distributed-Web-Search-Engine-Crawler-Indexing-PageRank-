(function () {
    function attachAutocomplete(inputId, boxId) {
        const input = document.getElementById(inputId);
        const box = document.getElementById(boxId);
        if (!input || !box) return;   // Skip if missing

        input.addEventListener("input", async () => {
            const prefix = input.value.trim();
            if (!prefix) {
                box.style.display = "none";
                box.innerHTML = "";
                return;
            }

            try {
                const resp = await fetch("/suggest?prefix=" + encodeURIComponent(prefix));
                if (!resp.ok) {
                    box.style.display = "none";
                    return;
                }
                const arr = await resp.json();

                if (!arr || arr.length === 0) {
                    box.style.display = "none";
                    box.innerHTML = "";
                    return;
                }

                box.innerHTML = "";
                arr.forEach(word => {
                    const div = document.createElement("div");
                    div.className = "suggest-item";
                    div.textContent = word;

                    // Use mousedown to avoid blur
                    div.addEventListener("mousedown", (e) => {
                        e.preventDefault();
                        input.value = word;
                        box.style.display = "none";
                    });

                    box.appendChild(div);
                });

                // Show dropdown
                box.style.display = "block";

            } catch (e) {
                // Hide dropdown on error
                box.style.display = "none";
                box.innerHTML = "";
            }
        });

        // Hide when clicking outside
        document.addEventListener("click", (e) => {
            if (e.target !== input && !box.contains(e.target)) {
                box.style.display = "none";
            }
        });
    }

    // Home page
    attachAutocomplete("home-query-input", "home-suggest-box");
    // Result page
    attachAutocomplete("result-query-input", "result-suggest-box");
})();
