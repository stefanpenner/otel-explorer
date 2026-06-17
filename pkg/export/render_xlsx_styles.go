package export

import "github.com/xuri/excelize/v2"

// xlsxStyles holds the reusable cell styles for the workbook, created once.
type xlsxStyles struct {
	title       int
	subtitle    int
	h2          int
	cardLabel   int
	cardSub     int
	cardValue   map[string]int // by tone
	tag         map[string]int // severity chip, by tone
	finding     int
	tableHeader int
	failCell    int
}

// tone color tokens (font, fill) shared with the HTML palette.
var toneFont = map[string]string{
	"good": "#1A7F37", "warn": "#9A6700", "bad": "#CF222E", "neutral": "#1B1F24",
}
var toneFill = map[string]string{
	"good": "#DAFBE1", "warn": "#FFF8C5", "bad": "#FFEBE9", "neutral": "#4C8BF5",
}

const cardBG = "#F7F9FB"
const lineColor = "#E7EBEF"

func newXLSXStyles(f *excelize.File) (*xlsxStyles, error) {
	mk := func(s *excelize.Style) (int, error) { return f.NewStyle(s) }

	cardBorder := []excelize.Border{
		{Type: "top", Color: lineColor, Style: 1},
		{Type: "bottom", Color: lineColor, Style: 1},
		{Type: "left", Color: lineColor, Style: 1},
		{Type: "right", Color: lineColor, Style: 1},
	}
	fill := func(color string) excelize.Fill {
		return excelize.Fill{Type: "pattern", Pattern: 1, Color: []string{color}}
	}

	st := &xlsxStyles{cardValue: map[string]int{}, tag: map[string]int{}}
	var err error

	if st.title, err = mk(&excelize.Style{Font: &excelize.Font{Bold: true, Size: 18}}); err != nil {
		return nil, err
	}
	if st.subtitle, err = mk(&excelize.Style{Font: &excelize.Font{Size: 10, Color: "#5B6772"}}); err != nil {
		return nil, err
	}
	if st.h2, err = mk(&excelize.Style{Font: &excelize.Font{Bold: true, Size: 13}}); err != nil {
		return nil, err
	}
	if st.cardLabel, err = mk(&excelize.Style{
		Font: &excelize.Font{Size: 9, Color: "#5B6772", Bold: true},
		Fill: fill(cardBG), Border: cardBorder,
		Alignment: &excelize.Alignment{Horizontal: "left", Vertical: "center"},
	}); err != nil {
		return nil, err
	}
	if st.cardSub, err = mk(&excelize.Style{
		Font: &excelize.Font{Size: 9, Color: "#5B6772"}, Fill: fill(cardBG), Border: cardBorder,
		Alignment: &excelize.Alignment{Horizontal: "left", Vertical: "top"},
	}); err != nil {
		return nil, err
	}
	for _, tone := range []string{"good", "warn", "bad", "neutral"} {
		color := toneFont[tone]
		if st.cardValue[tone], err = mk(&excelize.Style{
			Font: &excelize.Font{Bold: true, Size: 18, Color: color}, Fill: fill(cardBG), Border: cardBorder,
			Alignment: &excelize.Alignment{Horizontal: "left", Vertical: "center"},
		}); err != nil {
			return nil, err
		}
	}
	for _, tone := range []string{"good", "warn", "bad", "neutral"} {
		if st.tag[tone], err = mk(&excelize.Style{
			Font: &excelize.Font{Bold: true, Color: "#FFFFFF"}, Fill: fill(toneFill[tone]),
			Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"},
		}); err != nil {
			return nil, err
		}
	}
	if st.finding, err = mk(&excelize.Style{Alignment: &excelize.Alignment{Vertical: "center", WrapText: false}}); err != nil {
		return nil, err
	}
	if st.tableHeader, err = mk(&excelize.Style{Font: &excelize.Font{Bold: true}}); err != nil {
		return nil, err
	}
	if st.failCell, err = mk(&excelize.Style{Font: &excelize.Font{Color: "#CF222E", Bold: true}, Fill: fill("#FFEBE9")}); err != nil {
		return nil, err
	}
	return st, nil
}
