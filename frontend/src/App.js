import * as React from 'react';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Card from '@mui/material/Card';
import CardActions from '@mui/material/CardActions';
import CardContent from '@mui/material/CardContent';
import CardHeader from '@mui/material/CardHeader';
import CssBaseline from '@mui/material/CssBaseline';
import Grid from '@mui/material/Grid';
// import StarIcon from '@mui/icons-material/StarBorder';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import Link from '@mui/material/Link';
import GlobalStyles from '@mui/material/GlobalStyles';
import Container from '@mui/material/Container';
import { TextField } from '@mui/material';
import axios from './utils/axios'
// iport io from 'socket.io-client'
import io from 'socket.io-client'

// const socketIO = io("http://localhost:8001/")

function Copyright(props) {
  return (
    <Typography variant="body2" color="text.secondary" align="center" {...props}>
      {'Copyright © '}
      <Link color="inherit" >
        Distributed Load Testing
      </Link>{' '}
      {new Date().getFullYear()}
      {'.'}
    </Typography>
  );
}


// TODO remove, this demo shouldn't need to reset the theme.
const defaultTheme = createTheme();

// const socketIO = io("http://127.0.1.1:8001/", {
//   forceNew: true,
//   transports: ['websocket']
// })
export default function App() {
  const [delay, setDelay] = React.useState(0);
  const [count, setCount] = React.useState(100);
  const [testConfig, setTestConfig] = React.useState({});
  const [metrics, setMetrics] = React.useState({});
  const [generatedInterval, setGeneratedInterval] = React.useState(null);

  const interval = () => setInterval(() => {
    axios.post("/metrics/" + testConfig['test_id']).then((response) => {
      console.log(response.data)
      if (response.data['metrics'])
        setMetrics(response.data['metrics'])
    }).catch((error) => { })
  }, 1000)
  React.useEffect(() => {
    // socketIO.on("connect", (data) => console.log(data))
    // socketIO.on("stream_data", (data) => console.log(data))
    // console.log(socketIO)
    return () => {
      clearInterval(generatedInterval)
    }
  }, [])
  const handleAvalancheClick = () => {
    // console.log(socketIO)
    axios.post("/avalance/" + count).then(response => {
      console.log(response.data);
      setTestConfig(response.data);
    }
    )
  }

  const handleTsunamiClick = () => {
    axios.post("/tsunami/" + count + "/" + delay).then(response => {
      console.log(response.data);
      setTestConfig(response.data);
    }
    )
  }

  const handleTriggerClick = () => {
    axios.post("/trigger/" + testConfig['test_id']).then(response => {
      setMetrics({})
      setTimeout(() => {
      setGeneratedInterval(interval());
      }, 1000)
      // console.log(response.data);
      // setTestConfig(response.data);
      // socketIO.on("data", (data) => console.log(data));
    }
    )
  }
  return (
    <ThemeProvider theme={defaultTheme}>
      <GlobalStyles styles={{ ul: { margin: 0, padding: 0, listStyle: 'none' } }} />
      <CssBaseline />
      <AppBar
        position="static"
        color="default"
        elevation={0}
        sx={{ borderBottom: (theme) => `1px solid ${theme.palette.divider}` }}
      >
        <Toolbar sx={{ flexWrap: 'wrap' }}>
          <Typography variant="h6" color="inherit" noWrap sx={{ flexGrow: 1 }}>
            Distributed Load Testing
          </Typography>
        </Toolbar>
      </AppBar>
      {/* Hero unit */}
      <Container disableGutters maxWidth="xl" component="main" sx={{ pt: 8, pb: 6, display: 'flex', flex: 1, alignItems: 'center', justifyContent: 'center', alignContent: 'center', justifyContent: "center", flexDirection: 'column' }}>
        <Typography
          component="h1"
          variant="h2"
          align="center"
          color="text.primary"
          gutterBottom
        >
          TESTS
        </Typography>
        <Card sx={{ width: '50%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
          <CardContent sx={{ display: 'flex', alignContent: 'center', justifyContent: 'center', flexDirection: "column", width: '40%', gap: '1em' }}>
            <TextField
              label="Test Message Delay"
              onChange={(event) => setDelay(event.target.value)}
              required
              value={delay}
              onFocus={(event) => event.target.select()}
            />
            <TextField
              required
              label="Message Count Per Driver"
              onChange={(event) => setCount(event.target.value)}
              value={count}
              onFocus={(event) => event.target.select()}
            ></TextField>
            {/* "message_count_per_driver": count" */}
          </CardContent>
          <Grid sx={{ display: 'flex', flex: 1, flexDirection: 'row', justifyContent: 'center', gap: '10em', paddingBlock: '1em' }}>
            <Button onClick={handleAvalancheClick} variant='outlined'>AVALANCHE</Button>
            <Button onClick={handleTsunamiClick} variant='outlined'>TSUNAMI</Button>
          </Grid>
        </Card>
      </Container>
      {/* End hero unit */}
      <Container maxWidth="xl" component="main">
        <Grid container spacing={5} display='flex' flex={1} flexDirection='row' alignItems="center" justifyContent='space-evenly'>
          <Card sx={{ width: '40vw', height: '40vh' }}>
            <CardHeader
              title="Test Config"
              // subheader={tier.subheader}
              titleTypographyProps={{ align: 'center' }}
              // action={tier.title === 'Pro' ? <StarIcon /> : null}
              subheaderTypographyProps={{
                align: 'center',
              }}
              sx={{
                backgroundColor: (theme) =>
                  theme.palette.mode === 'light'
                    ? theme.palette.grey[200]
                    : theme.palette.grey[700],
              }}
            />
            <CardContent sx={{ height: '60%', display: 'flex', flex: 1, justifyContent: 'space-evenly', alignContent: 'space-evenly', gap: '1em' }}>
              <ul sx={{ display: 'flex', flex: 1, justifyContent: 'space-evenly', gap: '1em', justifyItems: 'center', }}>
                <Typography
                  component="li"
                  variant="subtitle1"
                  align="center"
                >
                  Test Type: &nbsp;
                  {testConfig['test_type'] || "No data available"}
                </Typography>
                <Typography
                  component="li"
                  variant="subtitle1"
                  align="center"
                >
                  Test ID: &nbsp;
                  {testConfig['test_id'] || "No data available"}
                </Typography>
                <Typography
                  component="li"
                  variant="subtitle1"
                  align="center"
                >
                  Count/node: &nbsp;
                  {testConfig['message_count_per_driver'] || "No data available"}
                </Typography>
                <Typography
                  component="li"
                  variant="subtitle1"
                  align="center"
                >
                  Delay: &nbsp;
                  {testConfig['test_message_delay']}
                </Typography>
              </ul>
            </CardContent>
            <CardActions sx={{ display: 'flex', alignContent: 'center', justifyContent: 'center' }}>
              <Button onClick={handleTriggerClick}>Trigger</Button>
            </CardActions>
          </Card>
          <Card sx={{ width: '40vw', height: '40vh' }}>
            <CardHeader
              title={"Metrics"}
              // subheader={tier.subheader}
              titleTypographyProps={{ align: 'center' }}
              // action={tier.title === 'Pro' ? <StarIcon /> : null}
              subheaderTypographyProps={{
                align: 'center',
              }}
              sx={{
                backgroundColor: (theme) =>
                  theme.palette.mode === 'light'
                    ? theme.palette.grey[200]
                    : theme.palette.grey[700],
              }}
            />
            <CardContent>
              <ul>
                <Typography
                  component="li"
                  variant="subtitle1"
                  align="center"
                >
                  Mean:&nbsp;
                  {metrics['mean'] || "No data avalable"}
                </Typography>
                <Typography
                  component="li"
                  variant="subtitle1"
                  align="center"
                >
                  Median:&nbsp;
                  {metrics['median'] || "No data avalable"}
                </Typography>
                <Typography
                  component="li"
                  variant="subtitle1"
                  align="center"
                >
                  Minimum: &nbsp;
                  {metrics['min'] || "No data avalable"}
                </Typography>
                <Typography
                  component="li"
                  variant="subtitle1"
                  align="center"
                >
                  Maximum:
                  {metrics['max'] || "No data available"}
                </Typography>
              </ul>
            </CardContent>
            <CardActions sx={{ display: 'flex', alignContent: 'center', justifyContent: 'center' }}>
              <Button onClick={() => { clearInterval(generatedInterval) }}>Stop</Button>
              <Button onClick={() => { setGeneratedInterval(interval()) }}>Start</Button>
            </CardActions>
          </Card>
        </Grid>
      </Container>
      {/* Footer */}
      <Container
        maxWidth="md"
        component="footer"
        sx={{
          borderTop: (theme) => `1px solid ${theme.palette.divider}`,
          mt: 8,
          py: [3, 6],
        }}
      >
        {/* <Grid container spacing={4} justifyContent="space-evenly">
          {footers.map((footer) => (
            <Grid item xs={6} sm={3} key={footer.title}>
              <Typography variant="h6" color="text.primary" gutterBottom>
                {footer.title}
              </Typography>
              <ul>
                {footer.description.map((item) => (
                  <li key={item}>
                    <Link href="#" variant="subtitle1" color="text.secondary">
                      {item}
                    </Link>
                  </li>
                ))}
              </ul>
            </Grid>
          ))}
        </Grid> */}
        <Copyright sx={{ mt: 5 }} />
      </Container>
      {/* End footer */}
    </ThemeProvider>
  );
}