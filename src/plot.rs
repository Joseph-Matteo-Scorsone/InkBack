use iced::{
    widget::{canvas, checkbox, column, container, row, scrollable, text, Canvas},
    Application, Color, Command, Element, Length, Point, Rectangle, Settings, Theme,
};

#[derive(Debug, Clone)]
pub struct EquityCurve {
    pub label: String,
    pub equity_data: Vec<f64>,
    pub visible: bool,
    pub color: Color,
}

pub struct EquityPlotter {
    equity_curves: Vec<EquityCurve>,
    benchmark: Option<Vec<f64>>,
    show_benchmark: bool,
}

#[derive(Debug, Clone)]
pub enum Message {
    ToggleCurve(usize),
    ToggleBenchmark,
}

impl Application for EquityPlotter {
    type Message = Message;
    type Theme = Theme;
    type Executor = iced::executor::Default;
    type Flags = (Vec<(String, Vec<f64>)>, Option<Vec<f64>>);

    fn new(flags: Self::Flags) -> (Self, Command<Self::Message>) {
        let (curves_data, benchmark) = flags;

        // Generate colors for each curve
        let colors = generate_colors(curves_data.len());

        let equity_curves: Vec<EquityCurve> = curves_data
            .into_iter()
            .enumerate()
            .map(|(i, (label, data))| EquityCurve {
                label,
                equity_data: data,
                visible: true,
                color: colors[i],
            })
            .collect();

        (
            Self {
                equity_curves,
                benchmark,
                show_benchmark: true,
            },
            Command::none(),
        )
    }

    fn title(&self) -> String {
        "InkBack from Scorsone Enterprises".to_string()
    }

    fn update(&mut self, message: Self::Message) -> Command<Self::Message> {
        match message {
            Message::ToggleCurve(index) => {
                if let Some(curve) = self.equity_curves.get_mut(index) {
                    curve.visible = !curve.visible;
                }
            }
            Message::ToggleBenchmark => {
                self.show_benchmark = !self.show_benchmark;
            }
        }
        Command::none()
    }

    fn view(&self) -> Element<'_, Self::Message> {
        let chart = Canvas::new(ChartRenderer {
            equity_curves: &self.equity_curves,
            benchmark: self.benchmark.as_ref(),
            show_benchmark: self.show_benchmark,
        })
        .width(Length::FillPortion(3))
        .height(Length::Fill);

        let controls = self.create_controls();

        row![
            chart,
            container(controls)
                .width(Length::FillPortion(1))
                .padding(20)
        ]
        .into()
    }

    fn theme(&self) -> Self::Theme {
        Theme::Dark
    }
}

impl EquityPlotter {
    fn create_controls(&self) -> Element<'_, Message> {
        let mut controls = column![
            text("Strategy Controls").size(20),
            text("Toggle visibility:").size(16),
        ]
        .spacing(10);

        // Add benchmark toggle if benchmark exists
        if self.benchmark.is_some() {
            controls = controls.push(
                checkbox("Show Benchmark", self.show_benchmark)
                    .on_toggle(|_| Message::ToggleBenchmark),
            );
        }

        // Add controls for each equity curve
        for (i, curve) in self.equity_curves.iter().enumerate() {
            let checkbox_widget = checkbox(&curve.label, curve.visible)
                .on_toggle(move |_| Message::ToggleCurve(i))
                .style(iced::theme::Checkbox::Custom(Box::new(CurveCheckboxStyle(
                    curve.color,
                ))));

            controls = controls.push(checkbox_widget);
        }

        scrollable(controls).into()
    }
}

struct CurveCheckboxStyle(Color);

impl iced::widget::checkbox::StyleSheet for CurveCheckboxStyle {
    type Style = Theme;

    fn active(&self, style: &Self::Style, is_checked: bool) -> iced::widget::checkbox::Appearance {
        let palette = style.palette();

        iced::widget::checkbox::Appearance {
            background: if is_checked {
                iced::Background::Color(self.0)
            } else {
                iced::Background::Color(palette.background)
            },
            icon_color: palette.text,
            text_color: Some(palette.text),
            border: iced::Border {
                color: if is_checked { self.0 } else { palette.text },
                width: 1.0,
                radius: 2.0.into(),
            },
        }
    }

    fn hovered(&self, style: &Self::Style, is_checked: bool) -> iced::widget::checkbox::Appearance {
        let mut appearance = self.active(style, is_checked);
        // Add slight transparency for hover effect
        appearance.background = match appearance.background {
            iced::Background::Color(color) => iced::Background::Color(Color {
                a: color.a * 0.8,
                ..color
            }),
            iced::Background::Gradient(_gradient) => todo!(),
        };
        appearance
    }
}

struct ChartRenderer<'a> {
    equity_curves: &'a [EquityCurve],
    benchmark: Option<&'a Vec<f64>>,
    show_benchmark: bool,
}

impl<'a> canvas::Program<Message> for ChartRenderer<'a> {
    type State = ();

    fn draw(
        &self,
        _state: &Self::State,
        renderer: &iced::Renderer,
        _theme: &Theme,
        bounds: Rectangle,
        _cursor: iced::mouse::Cursor,
    ) -> Vec<canvas::Geometry> {
        let mut frame = canvas::Frame::new(renderer, bounds.size());

        // Chart margins
        let margin = 80.0;
        let chart_bounds = Rectangle {
            x: margin,
            y: margin,
            width: bounds.width - 2.0 * margin,
            height: bounds.height - 2.0 * margin,
        };

        // Find global min/max for scaling
        let (min_val, max_val) = self.find_global_range();
        let max_length = self.find_max_length();

        if max_length == 0 {
            return vec![frame.into_geometry()];
        }

        // Draw grid and axes
        self.draw_grid_and_axes(&mut frame, &chart_bounds, min_val, max_val, max_length);

        // Draw benchmark if enabled
        if self.show_benchmark {
            if let Some(benchmark) = self.benchmark {
                self.draw_line(
                    &mut frame,
                    benchmark,
                    &chart_bounds,
                    min_val,
                    max_val,
                    max_length,
                    Color::WHITE,
                    2.0,
                );
            }
        }

        // Draw visible equity curves
        for curve in self.equity_curves.iter().filter(|c| c.visible) {
            self.draw_line(
                &mut frame,
                &curve.equity_data,
                &chart_bounds,
                min_val,
                max_val,
                max_length,
                curve.color,
                1.5,
            );
        }

        vec![frame.into_geometry()]
    }
}

impl<'a> ChartRenderer<'a> {
    fn find_global_range(&self) -> (f64, f64) {
        let mut min_val = f64::INFINITY;
        let mut max_val = f64::NEG_INFINITY;

        // Check visible equity curves
        for curve in self.equity_curves.iter().filter(|c| c.visible) {
            if let (Some(&curve_min), Some(&curve_max)) = (
                curve
                    .equity_data
                    .iter()
                    .min_by(|a, b| a.partial_cmp(b).unwrap()),
                curve
                    .equity_data
                    .iter()
                    .max_by(|a, b| a.partial_cmp(b).unwrap()),
            ) {
                min_val = min_val.min(curve_min);
                max_val = max_val.max(curve_max);
            }
        }

        // Check benchmark if shown
        if self.show_benchmark {
            if let Some(benchmark) = self.benchmark {
                if let (Some(&bench_min), Some(&bench_max)) = (
                    benchmark.iter().min_by(|a, b| a.partial_cmp(b).unwrap()),
                    benchmark.iter().max_by(|a, b| a.partial_cmp(b).unwrap()),
                ) {
                    min_val = min_val.min(bench_min);
                    max_val = max_val.max(bench_max);
                }
            }
        }

        // Add some padding
        let padding = (max_val - min_val) * 0.05;
        (min_val - padding, max_val + padding)
    }

    fn find_max_length(&self) -> usize {
        let mut max_len = 0;

        for curve in self.equity_curves.iter().filter(|c| c.visible) {
            max_len = max_len.max(curve.equity_data.len());
        }

        if self.show_benchmark {
            if let Some(benchmark) = self.benchmark {
                max_len = max_len.max(benchmark.len());
            }
        }

        max_len
    }

    fn draw_grid_and_axes(
        &self,
        frame: &mut canvas::Frame,
        bounds: &Rectangle,
        min_val: f64,
        max_val: f64,
        max_length: usize,
    ) {
        use iced::widget::canvas::{Path, Stroke, Text};

        // Draw axes
        let stroke = Stroke::default()
            .with_width(1.0)
            .with_color(Color::from_rgb(0.3, 0.3, 0.3));

        // Y-axis
        let y_axis = Path::line(
            Point::new(bounds.x, bounds.y),
            Point::new(bounds.x, bounds.y + bounds.height),
        );
        frame.stroke(&y_axis, stroke.clone());

        // X-axis
        let x_axis = Path::line(
            Point::new(bounds.x, bounds.y + bounds.height),
            Point::new(bounds.x + bounds.width, bounds.y + bounds.height),
        );
        frame.stroke(&x_axis, stroke);

        // Draw grid lines and labels
        let grid_stroke = Stroke::default()
            .with_width(0.5)
            .with_color(Color::from_rgb(0.2, 0.2, 0.2));

        // Horizontal grid lines for equity values
        for i in 0..=5 {
            let y_ratio = i as f32 / 5.0;
            let y = bounds.y + bounds.height * (1.0 - y_ratio);
            let value = min_val + (max_val - min_val) * y_ratio as f64;

            let grid_line = Path::line(
                Point::new(bounds.x, y),
                Point::new(bounds.x + bounds.width, y),
            );
            frame.stroke(&grid_line, grid_stroke.clone());

            // Y-axis labels
            let label = Text {
                content: format!("{:.0}", value),
                position: Point::new(bounds.x - 5.0, y),
                color: Color::WHITE,
                size: iced::Pixels(12.0),
                horizontal_alignment: iced::alignment::Horizontal::Right,
                vertical_alignment: iced::alignment::Vertical::Center,
                ..Default::default()
            };
            frame.fill_text(label);
        }

        // Vertical grid lines (for time)
        for i in 0..=5 {
            let x_ratio = i as f32 / 5.0;
            let x = bounds.x + bounds.width * x_ratio;
            let time_point = (max_length as f32 * x_ratio) as usize;

            let grid_line = Path::line(
                Point::new(x, bounds.y),
                Point::new(x, bounds.y + bounds.height),
            );
            frame.stroke(&grid_line, grid_stroke.clone());

            // X-axis labels
            let label = Text {
                content: format!("{}", time_point),
                position: Point::new(x, bounds.y + bounds.height + 15.0),
                color: Color::WHITE,
                size: iced::Pixels(12.0),
                horizontal_alignment: iced::alignment::Horizontal::Center,
                vertical_alignment: iced::alignment::Vertical::Top,
                ..Default::default()
            };
            frame.fill_text(label);
        }
    }

    fn draw_line(
        &self,
        frame: &mut canvas::Frame,
        data: &[f64],
        bounds: &Rectangle,
        min_val: f64,
        max_val: f64,
        max_length: usize,
        color: Color,
        width: f32,
    ) {
        use iced::widget::canvas::{Path, Stroke};

        if data.len() < 2 {
            return;
        }

        let max_render_points = 5000;
        let step = (data.len() / max_render_points).max(1);

        let path_builder = Path::new(|builder| {
            let value_range = max_val - min_val;

            // Iterate with step_by to skip points
            for (i, &value) in data.iter().enumerate().step_by(step) {
                // Calculate x based on the *original* index 'i' to maintain correct timeline
                let x = bounds.x + (i as f32 / (max_length - 1) as f32) * bounds.width;

                let y_ratio = if value_range != 0.0 {
                    ((value - min_val) / value_range) as f32
                } else {
                    0.5
                };
                let y = bounds.y + bounds.height * (1.0 - y_ratio);

                if i == 0 {
                    builder.move_to(Point::new(x, y));
                } else {
                    builder.line_to(Point::new(x, y));
                }
            }

            // Ensure the very last point is drawn if it wasn't covered by the step
            if step > 1 && !data.is_empty() {
                let last_val = data.last().unwrap();
                let x = bounds.x + bounds.width; // Far right
                let y_ratio = if value_range != 0.0 {
                    ((last_val - min_val) / value_range) as f32
                } else {
                    0.5
                };
                let y = bounds.y + bounds.height * (1.0 - y_ratio);
                builder.line_to(Point::new(x, y));
            }
        });

        let stroke = Stroke::default().with_width(width).with_color(color);
        frame.stroke(&path_builder, stroke);
    }
}

// Generate visually distinct colors for the curves
fn generate_colors(count: usize) -> Vec<Color> {
    let mut colors = Vec::with_capacity(count);

    for i in 0..count {
        let hue = ((i as f32) * 360.0 / count as f32) % 360.0;
        colors.push(hsv_to_rgb(hue, 0.85, 0.95));
    }

    colors
}

fn hsv_to_rgb(h: f32, s: f32, v: f32) -> Color {
    let c = v * s;
    let x = c * (1.0 - ((h / 60.0) % 2.0 - 1.0).abs());
    let m = v - c;

    let (r, g, b) = if h < 60.0 {
        (c, x, 0.0)
    } else if h < 120.0 {
        (x, c, 0.0)
    } else if h < 180.0 {
        (0.0, c, x)
    } else if h < 240.0 {
        (0.0, x, c)
    } else if h < 300.0 {
        (x, 0.0, c)
    } else {
        (c, 0.0, x)
    };

    Color::from_rgb(r + m, g + m, b + m)
}

// Main for application
pub fn run_equity_plotter(
    equity_curves: Vec<(String, Vec<f64>)>,
    benchmark: Option<Vec<f64>>,
) -> iced::Result {
    EquityPlotter::run(Settings::with_flags((equity_curves, benchmark)))
}

// Called from main
pub fn plot_equity_curves(equity_curves: Vec<(String, Vec<f64>)>, benchmark: Option<Vec<f64>>) {
    if let Err(e) = run_equity_plotter(equity_curves, benchmark) {
        eprintln!("Error running Iced application: {}", e);
    }
}
