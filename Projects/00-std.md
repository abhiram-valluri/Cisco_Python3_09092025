## 🐍 **PEP 8 Summary**

### 1. **Indentation & Line Length**

* Use **4 spaces per indentation** (no tabs).
* Keep lines ≤ **79 characters** (72 for docstrings/comments).

---

### 2. **Blank Lines**

* **2 blank lines** before top-level functions and classes.
* **1 blank line** between methods inside a class.

---

### 3. **Imports**

* One import per line.
  ✅ Good:

  ```python
  import os
  import sys
  ```

  ❌ Bad:

  ```python
  import os, sys
  ```
* Group order:

  1. Standard library
  2. Third-party
  3. Local application imports

---

### 4. **Naming Conventions**

* **Variables & functions:** `snake_case`
* **Classes:** `CamelCase`
* **Constants:** `UPPER_CASE`
* Don’t shadow built-ins (`id`, `list`, `dict`, etc.).

---

### 5. **Whitespace**

* No trailing whitespace.
* Spaces **around operators**:

  ```python
  x = y + 2
  ```
* No space directly inside parentheses/brackets:

  ```python
  my_list = [1, 2, 3]
  ```

---

### 6. **Docstrings**

* Use triple quotes (`"""`).
* Modules, classes, and functions should have docstrings.
  Example:

  ```python
  def add(x, y):
      """Return the sum of x and y."""
      return x + y
  ```

---

### 7. **Comparisons**

* Use `is None` / `is not None` instead of `== None` or `!= None`.
* For booleans:
  ✅ `if flag:`
  ❌ `if flag == True:`

---

### 8. **Error Handling**

* Use `try/except` for exceptions, not returning error codes.
* Don’t use bare `except:` → use `except Exception:`.

---

### 9. **Functions**

* Small and focused (avoid >50 statements).
* Break large functions into helpers.

---

### 10. **Comments**

* Write **meaningful comments**, not obvious ones.
* Inline comments start with **two spaces + #**.

  ```python
  x = x + 1  # Increment counter
  ```

---

✅ Following **PEP 8** will automatically reduce most pylint warnings.

```
```

## 🐍 **Beyond PEP 8 – Other Python Standards & Practices**

### 1. **PEP 257 – Docstring Conventions**

* Explains *how* to write docstrings properly (PEP 8 just says you need them).
* Example:

  ```python
  def fetch_employee(emp_id):
      """
      Fetch an employee by ID from the database.

      Args:
          emp_id (int): Unique employee identifier.

      Returns:
          dict: Employee details if found, else None.
      """
  ```

---

### 2. **PEP 20 – The Zen of Python**

* `import this` in Python shell → shows guiding principles.
  Key ones:

  * "Beautiful is better than ugly."
  * "Simple is better than complex."
  * "Readability counts."

---

### 3. **PEP 484 / PEP 561 – Type Hints**

* Add **type annotations** to improve readability & tooling.

  ```python
  def add(x: int, y: int) -> int:
      return x + y
  ```
* Use `mypy` to check types statically.

---

### 4. **Linting & Formatting Tools**

* **pylint** → strict code quality checker.
* **flake8** → lightweight linter for PEP 8 + errors.
* **black** → auto-formatter (PEP 8 compliant).
* **isort** → auto-sorts imports.

---

### 5. **Pythonic Code Conventions**

* Use list comprehensions instead of long loops:

  ```python
  nums_squared = [n*n for n in nums]
  ```
* Use built-ins like `any()`, `all()`, `sum()`, `enumerate()` instead of manual loops.
* Prefer `with open(...) as f:` for file handling (context managers).

---

### 6. **Code Complexity**

* Follow **Cyclomatic Complexity** rules (keep functions simple).
* Tools: `radon` → analyzes complexity.

---

### 7. **Testing Standards**

* **pytest/unittest** for unit tests.
* Follow **AAA rule** → Arrange, Act, Assert.
* Example:

  ```python
  def test_add():
      # Arrange
      x, y = 2, 3
      # Act
      result = add(x, y)
      # Assert
      assert result == 5
  ```

---

### 8. **Logging**

* Don’t use `print()` for debugging in production → use `logging`.

  ```python
  import logging
  logging.basicConfig(level=logging.INFO)
  logging.info("Employee created successfully.")
  ```

---

### 9. **Project Structure**

Typical clean layout:

```
project/
│── app/
│   ├── __init__.py
│   ├── models.py
│   ├── repo.py
│   ├── app.py
│── tests/
│   ├── test_app.py
│── requirements.txt
│── README.md
```

---

### 10. **Other Helpful Style Guides**

* **Google Python Style Guide** (very popular in industry).
* **Hitchhiker’s Guide to Python** (practical best practices).
* **PyTorch / Django internal guides** if you use frameworks.

---

✅ If you combine **PEP 8 (style)** + **PEP 257 (docstrings)** + **PEP 484 (typing)** + **linting/formatting tools**, your Python code will look professional and pass most code reviews.
