;; -*- lexical-binding: t; -*-
;;; ejc-corfu.el -- SQL completions for Corfu (the part of ejc-sql).

;;; Copyright © 2020 - 2026 Kostafey <kostafey@gmail.com>

;;; This program is free software; you can redistribute it and/or modify
;;; it under the terms of the GNU General Public License as published by
;;; the Free Software Foundation; either version 2, or (at your option)
;;; any later version.
;;;
;;; This program is distributed in the hope that it will be useful,
;;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;; GNU General Public License for more details.
;;;
;;; You should have received a copy of the GNU General Public License
;;; along with this program; if not, write to the Free Software Foundation,
;;; Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.  */

;;; Commentary:

;; `ejc-corfu' is a `corfu' completion backend for `ejc-sql'.
;; It uses Emacs's native `completion-at-point-functions' (CAPF).
;;
;; To use it:
;;
;;     (require 'corfu)
;;     (require 'ejc-corfu)
;;     (setq corfu-auto t)
;;     (setq ejc-complete-on-dot t)
;;     (add-hook 'ejc-sql-minor-mode-hook #'ejc-corfu-setup)
;;     (add-hook 'sql-mode-hook #'corfu-mode)

;;; Code:

(require 'cl-lib)
(require 'dash)
(require 'ejc-completion-common)

(defun ejc-corfu--make-candidate (candidate)
  "Create a completion candidate with metadata.
CANDIDATE is a cons cell (TEXT . META)."
  (let ((text (car candidate))
        (meta (cadr candidate)))
    (propertize text 'meta meta)))

(defun ejc-corfu--add-meta (meta candidates)
  "Add META information to CANDIDATES list."
  (-map (lambda (k) (list k meta))
        candidates))

(defun ejc-corfu--candidates (prefix)
  "Return SQL completion candidates for PREFIX.
Reuses candidate generation logic from `ejc-completion-common'."
  (let* ((prefix-1 (ejc-get-prefix-word))
         (prefix-2 (save-excursion
                     (search-backward "." nil t)
                     (ejc-get-prefix-word)))
         (res))
    (dolist (item
             (cl-remove-if-not
              (lambda (c) (string-prefix-p prefix (car c) t))
              (append
               (ejc-append-without-duplicates
                (ejc-corfu--add-meta
                 "ansi sql" (ejc-get-ansi-sql-words))
                (ejc-corfu--add-meta
                 "keyword" (ejc-get-keywords))
                'car :right)
               (ejc-corfu--add-meta
                "owner" (ejc-owners-candidates))
               (ejc-corfu--add-meta
                "table" (ejc-tables-candidates))
               (ejc-corfu--add-meta
                "view" (ejc-views-candidates))
               (if (not prefix-1)
                   (ejc-corfu--add-meta
                    "package" (ejc-packages-candidates)))
               (ejc-corfu--add-meta
                "column" (ejc-colomns-candidates)))))
      (push (ejc-corfu--make-candidate item) res))
    (nreverse res)))

(defun ejc-corfu--annotation (candidate)
  "Return annotation string for CANDIDATE.
Shows the type of completion (keyword, table, column, etc.)."
  (let ((meta (get-text-property 0 'meta candidate)))
    (format "  %s" (or meta ""))))

(defun ejc-corfu--doc-buffer (candidate)
  "Return documentation buffer for CANDIDATE."
  (let ((doc (ac-ejc-documentation candidate)))
    (when doc
      (with-current-buffer (get-buffer-create "*corfu-doc*")
        (erase-buffer)
        (insert doc)
        (goto-char (point-min))
        (current-buffer)))))

(defun ejc-corfu--doc-from-meta (candidate)
  "Return documentation for CANDIDATE using its meta property."
  (let ((meta (get-text-property 0 'meta candidate)))
    (when meta
      (with-current-buffer (get-buffer-create "*corfu-doc*")
        (erase-buffer)
        (insert meta)
        (goto-char (point-min))
        (current-buffer)))))

;;;###autoload
(defun ejc-corfu-capf ()
  "Completion at point function (CAPF) for Corfu.
Returns a tuple compatible with `completion-at-point-functions'."
  (let ((bounds (bounds-of-thing-at-point 'symbol)))
    (when bounds
      (let ((prefix (buffer-substring-no-properties (car bounds) (cdr bounds))))
        (list (car bounds)
              (cdr bounds)
              (ejc-corfu--candidates prefix)
              :annotation-function #'ejc-corfu--annotation
              :company-doc-buffer #'ejc-corfu--doc-buffer
              :company-meta (lambda (cand) (get-text-property 0 'meta cand))))))

;;;###autoload
(defun ejc-corfu-setup ()
  "Setup Corfu integration for ejc-sql.
Adds `ejc-corfu-capf' to `completion-at-point-functions' locally."
  (add-hook 'completion-at-point-functions #'ejc-corfu-capf nil t))

;;;###autoload
(defun ejc-corfu-complete-on-dot ()
  "Insert a dot and trigger Corfu completion.
Used when `ejc-complete-on-dot' is enabled."
  (interactive)
  (insert ".")
  (when (and ejc-complete-on-dot
             (bound-and-true-p corfu-mode))
    ;; Trigger completion at point - Corfu will pick it up via CAPF
    (completion-at-point)))

(provide 'ejc-corfu)

;;; ejc-corfu.el ends here
