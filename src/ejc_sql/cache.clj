;;; cache.clj -- Keep database stucture cache.

;;; Copyright © 2019-2023 - Kostafey <kostafey@gmail.com>

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

(ns ejc-sql.cache
  (:require [clomacs :refer [clomacs-defn]]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as s])
  (:import [java.io File]))

(def cache
  "Keep information about structure of databases used for autocomplete & eldoc.
  This data contains (depends on database type):
  ├── owners/schemas
  |   ├── tables
  |   |   └── columns
  |   └── views
  ├── packages
  |   └── stored procedures & functions
  |       └── parameters
  └── keywords
  It's global: same database structure information shared beetween
  different buffers connected to the same database."
  (atom {}))

(def cache-creation-promises
  "Signs of database structure cache created."
  (atom {}))

;; Cache persistence configuration
(def ^:dynamic *cache-dir*
  "Directory to store cache files. Can be overridden with set-cache-dir!"
  (str (System/getProperty "user.home") "/.emacs.d/ejc-sql-cache/"))

(defn set-cache-dir!
  "Set the cache directory. Creates the directory if it doesn't exist."
  [dir]
  (let [cache-dir-file (io/file dir)]
    (.mkdirs cache-dir-file)
    (alter-var-root #'*cache-dir* (constantly dir))))

;; Initialize cache directory
(set-cache-dir! *cache-dir*)

(defn get-connection-fingerprint
  "Generate a unique fingerprint for a database connection.
   Uses connection details to create a stable identifier."
  [db]
  (let [{:keys [subprotocol subname connection-uri dbname user host port]} db]
    (s/join "_"
            (remove nil?
                    [(or subprotocol "unknown")
                     (or dbname "unknown")
                     (or user "unknown")
                     (or host "unknown")
                     (or port "unknown")
                     (when subname
                       (-> subname
                           (s/split #"/" -1)
                           last
                           (s/split #"\?" 2)
                           first))]))))

(defn get-cache-file-path
  "Get the file path for a connection's cache file."
  [db]
  (let [fingerprint (get-connection-fingerprint db)]
    (io/file *cache-dir* (str fingerprint ".edn"))))

(defn resolve-cache-futures
  "Recursively resolve all futures in the cache data structure.
  Returns a data structure with only concrete values (no futures)."
  [data]
  (cond
    (future? data)
    (try @data (catch Exception _ nil))

    (map? data)
    (into {} (map (fn [[k v]] [k (resolve-cache-futures v)]) data))

    (coll? data)
    (mapv resolve-cache-futures data)

    :else
    data))

(defn prepare-cache-for-saving
  "Prepare cache data for serialization by resolving futures and filtering nils."
  [db]
  (when-let [cache-data (get @cache db)]
    (let [resolved-cache (resolve-cache-futures cache-data)]
      ;; Remove empty collections and nil values
      (into {}
            (remove (fn [[_ v]]
                      (or (nil? v)
                          (and (coll? v) (empty? v))))
                    resolved-cache)))))

(defn save-cache-to-file!
  "Save the cache for a specific database connection to file."
  [db]
  (try
    (when-let [cache-data (prepare-cache-for-saving db)]
      (let [cache-file (get-cache-file-path db)]
        (io/make-parents cache-file)
        (spit cache-file (pr-str cache-data))
        true))
    (catch Exception e
      (println (str "Failed to save cache: " (.getMessage e)))
      false)))

(defn load-cache-from-file!
  "Load the cache for a specific database connection from file."
  [db]
  (try
    (let [cache-file (get-cache-file-path db)]
      (when (.exists cache-file)
        (let [cache-data (edn/read-string (slurp cache-file))]
          (when (map? cache-data)
            (swap! cache assoc db cache-data)
            true))))
    (catch Exception e
      (println (str "Failed to load cache: " (.getMessage e)))
      false)))

(defn delete-cache-file!
  "Delete the cache file for a specific database connection."
  [db]
  (try
    (let [cache-file (get-cache-file-path db)]
      (when (.exists cache-file)
        (.delete cache-file)
        true))
    (catch Exception e
      (println (str "Failed to delete cache file: " (.getMessage e)))
      false)))

(defn save-cache-when-ready!
  "Save cache to file when all async operations are complete."
  [db]
  (future
    ;; Wait for all cache building promises to complete
    (let [promises (vals (get-in @cache-creation-promises [db]))]
      (when (seq promises)
        (doseq [promise promises]
          (try @promise (catch Exception _))))
      ;; Save the resolved cache
      (save-cache-to-file! db))))

;; Clomacs functions for Emacs integration
(clomacs-defn
 save-cache-when-ready
 save-cache-when-ready!
 :doc "Save cache to file when all async operations are complete.")

(clomacs-defn
 load-cache-from-file
 load-cache-from-file!
 :doc "Load cache from file for a database connection.")

(clomacs-defn
 delete-cache-file
 delete-cache-file!
 :doc "Delete cache file for a database connection.")

(clomacs-defn
 set-cache-dir
 set-cache-dir!
 :doc "Set the cache directory path.")

(defn get-cache
  "Get actual cache."
  []
  @cache)

(defn deref-cache [db]
  [db
   (into
    {}
    (map
     (fn [entity-k]
       [entity-k
        (let [item (get-in (get-cache) [db entity-k])]
          (if (future? item)
            (deref item)
            (if (map? item)
              (into
               {}
               (map
                (fn [sub-entity-k]
                  [sub-entity-k
                   (let [sub-item (get-in (get-cache)
                                          [db entity-k sub-entity-k])]
                     (if (future? sub-item)
                       (deref sub-item)
                       sub-item))])
                (keys item)))
              item)))])
     (keys
      ((get-cache) db))))])

(defn output-cache
  "Output actual cache to printable format."
  [db]
  (clomacs/format-result
   (if db
     (second (deref-cache db))
     (into {} (map deref-cache (keys (get-cache)))))))

(defn invalidate-cache
  "Clean your current connection cache (database owners and tables list).
   Also deletes the persisted cache file."
  [db]
  ;; Delete cache file first
  (delete-cache-file! db)
  ;; Clear in-memory cache
  (swap! cache assoc-in [db] nil)
  (swap! cache-creation-promises assoc-in [db] nil))
