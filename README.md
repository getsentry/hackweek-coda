<div align="center">
  <img src="https://github.com/mitsuhiko/coda/blob/main/assets/logo.png?raw=true" alt="" width=320>
  <p><strong>Coda:</strong> capisci le code?</p>
</div>

Celery just less terrible.

## Todo

* [ ] Implement workflow retries
* [ ] Add a way to spawn a workflow from outside a supervisor via TCP socket (`coda.Client().spawn_workflow()`)
* [ ] Delete state of ended workflows and their tasks
* [ ] Add TTLing of workflows