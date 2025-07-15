import tkinter as tk
from tkinter import messagebox, Scrollbar
from tkinter.scrolledtext import ScrolledText
import tkinter.filedialog as filedialog
from azure.servicebus import ServiceBusClient
import pandas as pd
import json
from tabulate import tabulate
import duckdb


class QueueViewerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Azure Service Bus Queue Viewer")

        # Register a validation command
        vcmd = root.register(self.validate_digits)

        # Queue name label
        queue_label = tk.Label(root, text="Queue name")
        queue_label.grid(row=0, column=0, padx=(10, 2), pady=10, sticky='e')

        # Queue name textbox
        self.queue_name = tk.StringVar()
        queue_entry = tk.Entry(root, textvariable=self.queue_name, width=30)
        queue_entry.grid(row=0, column=1, padx=(2, 5), pady=10, sticky='w')

        # Environment label
        env_label = tk.Label(root, text="Environment")
        env_label.grid(row=0, column=2, padx=(0, 2), pady=10, sticky='w')

        # Environment dropdown
        self.environment = tk.StringVar(value=EnvironmentList[0])
        env_dropdown = tk.OptionMenu(root, self.environment, *EnvironmentList)
        env_dropdown.config(width=10)
        env_dropdown.grid(row=0, column=2, padx=(2, 5), pady=10, sticky='e')

        # Radiobuttons
        self.queue_type = tk.StringVar(value="queue")
        queue_radio = tk.Radiobutton(root, text="Queue", variable=self.queue_type, value="queue")
        queue_radio.grid(row=0, column=3, padx=(0, 2), pady=10, sticky='e')

        deadletter_radio = tk.Radiobutton(root, text="Dead-Letter", variable=self.queue_type, value="deadletter")
        deadletter_radio.grid(row=0, column=4, padx=(2, 5), pady=10, sticky='w')

        # Checkbox
        self.display_full_msg = tk.BooleanVar(value=False)
        full_msg_checkbox = tk.Checkbutton(root, text="Display full message", variable=self.display_full_msg)
        full_msg_checkbox.grid(row=0, column=5, padx=(5, 5), pady=10, sticky='w')

        # Load button
        load_button = tk.Button(root, text="Load", command=self.load_messages, width=10)
        load_button.grid(row=0, column=6, padx=(5, 10), pady=10, sticky='w')

        # Output text area
        output_frame = tk.Frame(root)
        output_frame.grid(row=1, column=0, columnspan=7, padx=10, pady=10, sticky='nsew')

        self.output_area = ScrolledText(output_frame, width=150, height=20, wrap='none')
        self.output_area.grid(row=0, column=0, sticky='nsew')

        x_scroll = Scrollbar(output_frame, orient='horizontal', command=self.output_area.xview)
        x_scroll.grid(row=1, column=0, sticky='ew')
        self.output_area.configure(xscrollcommand=x_scroll.set, state='disabled')

        output_frame.grid_rowconfigure(0, weight=1)
        output_frame.grid_columnconfigure(0, weight=1)

        # Store last result
        self.last_df = pd.DataFrame()

        # Label and Entry for min sequence number
        min_seq_label = tk.Label(root, text="Min Sequence number")
        min_seq_label.grid(row=2, column=0, padx=(10, 5), pady=(0, 10), sticky='e')
        self.min_sequence_num = tk.StringVar()
        self.min_seq_entry = tk.Entry(root, textvariable=self.min_sequence_num, width=15, validate='key', validatecommand=(vcmd, '%P'))
        self.min_seq_entry.grid(row=2, column=1, padx=(0, 20), pady=(0, 10), sticky='w')

        # Label and Entry for max sequence number
        max_seq_label = tk.Label(root, text="Max Sequence number")
        max_seq_label.grid(row=2, column=1, padx=(10, 5), pady=(0, 10), sticky='e')
        self.max_sequence_num = tk.StringVar()
        self.max_seq_entry = tk.Entry(root, textvariable=self.max_sequence_num, width=15, validate='key', validatecommand=(vcmd, '%P'))
        self.max_seq_entry.grid(row=2, column=2, padx=(0, 20), pady=(0, 10), sticky='w')

        # Export button
        export_button = tk.Button(root, text="Export to Excel", command=self.export_to_excel, width=20)
        export_button.grid(row=2, column=4, padx=(10, 5), pady=(0, 10), sticky='e')

        # Reset button
        reset_button = tk.Button(root, text="Reload", command=self.reset, width=20)
        reset_button.grid(row=2, column=5, padx=(5, 10), pady=(0, 10), sticky='e')

        # Query section
        query_frame = tk.Frame(root)
        query_frame.grid(row=3, column=0, columnspan=7, sticky='we', padx=10, pady=(10, 10))

        # Query label
        query_label = tk.Label(query_frame, text="Enter Query here\n\n1. Use 'azq' as your table name\n2. Follows DuckDB SQL syntax", justify='center')
        query_label.grid(row=0, column=0, rowspan=2, sticky='nw')

        # Execute button (disabled by default)
        button_frame = tk.Frame(query_frame)
        button_frame.grid(row=2, column=0, pady=(10, 0))

        self.execute_button = tk.Button(button_frame, text="Execute Query", state='disabled', command=self.execute_query, width=15)
        self.execute_button.pack(anchor='center')

        # Query input area
        self.query_input = tk.Text(query_frame, height=5, width=80)
        self.query_input.grid(row=0, column=1, rowspan=3, sticky='nsew', padx=(10, 0))

        query_frame.grid_columnconfigure(1, weight=1)

    def load_messages(self):
        queue_name = self.queue_name.get().strip()
        if not queue_name:
            messagebox.showerror("Error", "Please enter a queue name.")
            return

        target_queue = queue_name
        if self.queue_type.get() == "deadletter":
            target_queue += "/$DeadLetterQueue"

        min_sequence_number = self.min_sequence_num.get().strip()
        max_sequence_number = self.max_sequence_num.get().strip()
        get_from_sequence_range = False
        sequence_range = 0

        if (min_sequence_number != "" and max_sequence_number != ""):
            get_from_sequence_range = True
            min_sequence_number = int(min_sequence_number)
            max_sequence_number = int(max_sequence_number)
            sequence_range = max_sequence_number - min_sequence_number

        try:
            messages_data = []
            connection_string = ServiceBusConnection[self.environment.get()]
            with ServiceBusClient.from_connection_string(connection_string) as client:
                receiver = client.get_queue_receiver(queue_name=target_queue)
                with receiver:
                    all_messages = []
                    last_seq_num = min_sequence_number if get_from_sequence_range else None

                    while (get_from_sequence_range == False or last_seq_num < max_sequence_number):
                        if last_seq_num is None:
                            messages = receiver.peek_messages(max_message_count = sequence_range if (get_from_sequence_range and sequence_range < 300) else 300)
                        else:
                            messages = receiver.peek_messages(max_message_count = sequence_range if (get_from_sequence_range and sequence_range < 300) else 300, sequence_number = last_seq_num + 1)

                        if not messages: break

                        all_messages.extend(messages)
                        last_seq_num = messages[-1].sequence_number

                    count = 1
                    for msg in all_messages:
                        app_props = msg.application_properties or {}

                        msg_object = {
                            "id": count,
                            "sequence_num": str(msg.sequence_number) if msg.sequence_number else "",
                            "message_id": str(msg.message_id) if msg.message_id else "",
                            "message": str(msg).replace('\n', ' ').replace('\r', '') if msg else "",
                            "enqueued_time_utc": msg.enqueued_time_utc.replace(tzinfo=None) if msg.enqueued_time_utc else None
                        }

                        if self.queue_type.get() == "deadletter":
                            msg_object["dead_letter_reason"] = app_props.get(b'DeadLetterReason', b'').decode()
                            msg_object["dead_letter_error_description"] = app_props.get(b'DeadLetterErrorDescription', b'').decode()

                        messages_data.append(msg_object)
                        count += 1

            self.last_df = pd.DataFrame(messages_data)
            self.display_table(self.last_df)

        except Exception as e:
            messagebox.showerror("Error", "Failed to receive messages.")
            print(f"Error: {e}")
            self.last_df = pd.DataFrame()
            self.execute_button.config(state='disabled')

    def export_to_excel(self):
        try:
            if not self.last_df.empty:
                file_path = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                         filetypes=[("Excel files", "*.xlsx")],
                                                         title="Save as")
                if file_path:
                    self.last_df.to_excel(file_path, index=False)
                    messagebox.showinfo("Info", "Data exported successfully!")
            else:
                messagebox.showinfo("Info", "No data to export.")
        except Exception as e:
            messagebox.showerror("Error", "Failed to export.")

    def execute_query(self):
        try:
            sql_query = self.query_input.get("1.0", tk.END).strip()

            if not self.last_df.empty:
                duckdb.register("azq", self.last_df)
                output_df = duckdb.sql(sql_query).df()
                self.display_table(output_df)
            else:
                messagebox.showinfo("Info", "No data in queue.")
        except Exception as e:
            messagebox.showerror("Error", "Query execution failed.")

    def display_table(self, df):
        self.execute_button.config(state='disabled')
        display_df = df.copy()

        # Truncate column contents
        if not display_df.empty:
            display_df["message_id"] = display_df["message_id"].astype(str)

            if self.display_full_msg.get():
                display_df["message"] = display_df["message"].astype(str).str.replace('\n', ' ')
            else:
                display_df["message"] = display_df["message"].astype(str).str.replace('\n', ' ').str.slice(0, 20)

            if self.queue_type.get() == "deadletter":
                display_df["dead_letter_reason"] = display_df["dead_letter_reason"].astype(str)
                display_df["dead_letter_error_description"] = display_df["dead_letter_error_description"].astype(str)

            # Format as table
            output = tabulate(display_df, headers="keys", tablefmt="grid", showindex=False)

            # Show in text area
            self.output_area.configure(state='normal')
            self.output_area.delete(1.0, tk.END)
            self.output_area.insert(tk.END, output)
            self.output_area.configure(state='disabled')
            self.execute_button.config(state='normal')
        else:
            messagebox.showinfo("Info", "Queue is empty.")
            self.execute_button.config(state='disabled')

    def reset(self):
        try:
            self.display_table(self.last_df)
        except Exception as e:
            messagebox.showerror("Error", "Failed to reset.")

    def validate_digits(self, new_value):
        """Allow only empty or digits in entry field"""
        return new_value.isdigit() or new_value == ""


if __name__ == "__main__":
    json_data = None
    with open('appsettings.json', 'r') as j:
        json_data = json.load(j)

    global ServiceBusConnection
    ServiceBusConnection = dict(json_data["ServiceBusConnection"])

    global EnvironmentList
    EnvironmentList = list(ServiceBusConnection.keys())

    root = tk.Tk()
    root.resizable(False, False)    # Disable window resizing (both x and y)
    app = QueueViewerApp(root)
    root.mainloop()
