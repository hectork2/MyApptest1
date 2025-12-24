require([
    "jquery",
    "splunkjs/mvc",
    "splunkjs/mvc/simplexml/ready!"], function($, mvc) {
        const FIRST_CONVERT_DROPDOWN_VALUE = 'table';
        var defaultToken = mvc.Components.get("default");
        var submittedTokenModel = mvc.Components.get("submitted");

        const tableEls = (() => {
          const arr = [...document.getElementsByClassName('splunk-view splunk-table')];
          arr.shift();
          return arr.map(el => el.children[1]);
        })();

        const clearDataPreviewPanel = () => {
          defaultToken.unset('data_query');
          submittedTokenModel.unset('data_query');
          tableEls[0].style.display = 'none';

        }

        const clearConvertTextToSPLPanel = () => {
          const convertSPLClearBtn = document.querySelector("#Convert_SPL_Panel [data-test=button][aria-label='Clear filter']");
          if (convertSPLClearBtn) {
            convertSPLClearBtn.click();
            defaultToken.unset('task_description_init');
            submittedTokenModel.unset('task_description_init');
            defaultToken.unset('form.task_description_init');
            submittedTokenModel.unset('form.task_description_init');
            defaultToken.unset('task_description');
            submittedTokenModel.unset('task_description');
            tableEls[1].style.display = 'none';
          }
        };

        const clearRunSearchPanel = () => {
          defaultToken.unset('input_query');
          submittedTokenModel.unset('input_query');
          defaultToken.unset('input_query_init');
          submittedTokenModel.unset('input_query_init');
          defaultToken.unset('form.input_query_init');
          submittedTokenModel.unset('form.input_query_init');
          tableEls[2].style.display = 'none';
        };

        const onDataIndexFieldClick = (e) => {
          clearDataPreviewPanel();
          clearConvertTextToSPLPanel();
          clearRunSearchPanel();
        };

        const onConvertFieldClick = (e) => {
          clearRunSearchPanel();
          $("#Convert_SPL_Panel [data-test=button][aria-label='Clear filter']").on('click', (e) => {
            clearRunSearchPanel();
          });
        };

        // Listeners for button clicks to reset table views
        $('#submit_data_query').on('click', (e) => {
          const data_query= $('#data_query input[type="text"]').val();
          submittedTokenModel.set("data_query", data_query);
          tableEls[0].style.display = 'block';
          clearConvertTextToSPLPanel();
          clearRunSearchPanel();
        });
        $('#submit_task_description').on('click', (e) => {
          const task_description= $('#task_description input[type="text"]').val();
          submittedTokenModel.set("task_description", task_description);
          tableEls[1].style.display = 'block';
          clearRunSearchPanel();
        });
        $('#submit_input_query').on('click', (e) => {
          const input_query= $('#input_query input[type="text"]').val();
          submittedTokenModel.set("input_query", input_query);
          tableEls[2].style.display = 'block';
        });

        // On field selection click
        $('div[data-test=select] button[data-test=toggle]').on('click', (e) => {
          // Need timeout for race condition of page rendering
          setTimeout(() => {
            if (e.currentTarget.closest('#Data_Preview_Panel')) {
              const fieldInputs = document.querySelectorAll('button[data-test=option][data-selectable=true]');
              fieldInputs.forEach(el => {
                el.addEventListener('click', onDataIndexFieldClick);
              });
            }
            if (e.currentTarget.closest('#Convert_SPL_Panel')) {
              const fieldInputs = document.querySelectorAll('button[data-test=option][data-selectable=true]');
              if (fieldInputs[0].value === FIRST_CONVERT_DROPDOWN_VALUE) {
                fieldInputs.forEach(el => {
                  el.addEventListener('click', onConvertFieldClick);
                });
              }
            }
          }, 0);
        });
    });
