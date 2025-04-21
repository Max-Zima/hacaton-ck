// script.js
document.addEventListener('DOMContentLoaded', () => {
    const dashBtn = document.getElementById('open-dashboard');
    dashBtn.addEventListener('click', () => {
        window.open('/dashboard/', '_blank');
    });

    const uploadForm = document.getElementById('upload-form');
    const fileInput = uploadForm.querySelector('input[type="file"]');
    const uploadBtn = uploadForm.querySelector('button[type="submit"]');

    // Метка для имени файла
    const fileNameLabel = document.createElement('span');
    fileNameLabel.className = 'file-name';
    fileNameLabel.style.marginRight = '10px';
    uploadForm.insertBefore(fileNameLabel, uploadBtn);

    uploadBtn.disabled = true;
    fileInput.addEventListener('change', () => {
        if (fileInput.files.length) {
            fileNameLabel.textContent = `Выбрано: ${fileInput.files[0].name}`;
            uploadBtn.disabled = false;
        } else {
            fileNameLabel.textContent = '';
            uploadBtn.disabled = true;
        }
    });

    uploadForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        if (!fileInput.files.length) return;

        uploadBtn.disabled = true;
        uploadBtn.textContent = 'Загружаю…';

        const formData = new FormData();
        formData.append('file', fileInput.files[0]);

        try {
            const res = await fetch('/upload/', {
                method: 'POST',
                body: formData
            });
            const data = await res.json();

            if (res.ok && data.message) {
                showToast(data.message, 'success');
            } else {
                showToast(data.error || 'Ошибка при загрузке', 'error');
            }
        } catch (err) {
            showToast(`Сетевая ошибка: ${err.message}`, 'error');
        } finally {
            uploadBtn.disabled = false;
            uploadBtn.textContent = 'Загрузить и обработать';
            fileInput.value = '';
            fileNameLabel.textContent = '';
        }
    });

    // Функция показа «toast»
    function showToast(message, type = 'info') {
        const toast = document.createElement('div');
        toast.className = `toast toast--${type}`;
        toast.textContent = message;
        document.body.appendChild(toast);

        // Включаем анимацию появления
        requestAnimationFrame(() => toast.classList.add('toast--visible'));

        // Удаляем через 4 секунды
        setTimeout(() => {
            toast.classList.remove('toast--visible');
            toast.addEventListener('transitionend', () => toast.remove(), { once: true });
        }, 4000);
    }
});
