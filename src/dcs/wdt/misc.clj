(ns dcs.wdt.misc)

(defn envvar [name]
  (if-let [value (System/getenv name)]
    value
    (throw (AssertionError. (str "Expected the environment variable " name " to have been defined")))))
